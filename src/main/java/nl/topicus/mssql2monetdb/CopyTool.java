package nl.topicus.mssql2monetdb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.math.BigDecimal;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class CopyTool {
	static Logger log = Logger.getLogger(CopyTool.class);
	
	public static final int DEFAULT_BATCH_SIZE = 10000;
	
	private Properties config;
	
	private Connection mssqlConn;
	private Connection monetDbConn;
	
	private Map<String, CopyTable> tablesToCopy = new HashMap<String, CopyTable>();
	
	private int batchSize = DEFAULT_BATCH_SIZE;
	
	public static String prepareMonetDbIdentifier (String ident) {
		// MonetDB only supports lowercase identifiers
		ident = ident.toLowerCase();
		
		// MonetDB doesn't support any special characters so replace with underscore
		ident = ident.replaceAll("[^a-zA-Z0-9]+","_");
		
		return ident;
	}
	
	public static String quoteMonetDbValue (String value) {
		return "'" + value.replaceAll("\\\\", "\\\\\\\\").replaceAll("'", "\\\\'") + "'";
	}
	
	public static String quoteMonetDbIdentifier (String ident) {
		// prepare identifier
		ident = prepareMonetDbIdentifier(ident);
		
		// make sure identifier is actually quoted
		ident = "\"" + ident + "\"";
		
		return ident;
	}
	
	public CopyTool (Properties config) {
		this.config = config;
		this.validateConfig();	
		this.findTablesToCopy();
	}
	
	public int getBatchSize () {
		return this.batchSize;
	}
	
	public void run () {
		if (tablesToCopy.size() > 0) {
			this.openConnections();
		
			for(CopyTable table : tablesToCopy.values()) {
				try {
					copyTable(table);
				} catch (SQLException e) {
					log.error("Unable to copy data from table '" + table.getFromName() + "'", e);
				}
			}
		
			this.closeConnections();
		}
		
		log.info("Finished!");
	}
	
	protected void copyTable(CopyTable table) throws SQLException {
		log.info("Starting with copy of table " + table.getFromName() + "...");
		
		// select data from MS SQL Server
		Statement selectStmt = mssqlConn.createStatement();

		// get number of rows in table
		ResultSet resultSet = selectStmt.executeQuery("SELECT COUNT(*) FROM [" + table.getFromName() + "]");
		resultSet.next();
		
		long rowCount = resultSet.getLong(1);
		log.info("Found " + rowCount + " rows in table " + table.getFromName());
		
		resultSet.close();
		
		// get all data from table
		resultSet = selectStmt.executeQuery("SELECT * FROM [" + table.getFromName() + "]");
		
		// get meta data (column info and such)
		ResultSetMetaData metaData = resultSet.getMetaData();
		
		// check table in monetdb
		checkTableInMonetDb(table, metaData);
		
		// do truncate?
		if (table.truncate()) {
			truncateTable(table);
		}
		
		// copy data
		try {
			copyData(table, resultSet, metaData, rowCount);
		} catch (SQLException e) {
			log.error("Copying data failed", e);
			
			// print full chain of exceptions
			SQLException nextException = e.getNextException();
			while(nextException != null) {
				nextException.printStackTrace();
				nextException = nextException.getNextException();
			}
		}
		
		// close everything again
		resultSet.close();
		selectStmt.close();
		
		log.info("Finished copy of table " + table.getFromName());
	}
	
	protected void checkTableInMonetDb (CopyTable table, ResultSetMetaData metaData) throws SQLException {
		// check if table exists
		Statement q = monetDbConn.createStatement();
		boolean tableExists = true;
		try {
			q.executeQuery("SELECT * FROM " + table.getToTableSql() + " LIMIT 1");
		} catch (SQLException e) {
			if (e.getMessage().indexOf("no such table") > -1) {
				// ok, so does not exist
				tableExists = false;
			} else {
				throw e;
			}
		}
		
		// can't auto create?
		if (tableExists == false && table.create() == false) {
			throw new SQLException("Table " + table.getToTableSql() + " does not exist in MonetDB database and auto-create is set to false");
		}
		
		// need to drop?
		if (tableExists && table.drop()) {
			log.info("Dropping table " + table.getToTableSql() + " in MonetDB database...");
			q.executeUpdate("DROP TABLE " + table.getToTableSql());
			tableExists = false;
			log.info("Table dropped");
		}
		
		if (tableExists) {
			// verify table is as expected
			this.verifyExistingTable(table, metaData);
		} else {
			// build SQL query to create table
			log.info("Creating table " + table.getToTableSql()  + " on MonetDB server...");
			StringBuilder createSql = new StringBuilder("CREATE TABLE " + table.getToTableSql() + " (");
			
			for(int i=1; i <= metaData.getColumnCount(); i++) {
				createSql.append(createColumnSql(i, metaData));
				createSql.append(",");
			}
			createSql.deleteCharAt(createSql.length()-1);			
			createSql.append(")");
			
			// execute CREATE TABLE SQL query
			q.execute(createSql.toString());
			log.info("Table created");
		}
	}
	
	protected void verifyExistingTable(CopyTable table, ResultSetMetaData metaData) throws SQLException {
		log.info("Verifying existing table " + table.getToTableSql() + " in MonetDB matches table schema in MS SQL...");
		
		// do a select on the table in MonetDB to get its metadata
		Statement q = monetDbConn.createStatement();
		ResultSet res = q.executeQuery("SELECT * FROM " + table.getToTableSql() + " LIMIT 1");
		ResultSetMetaData monetDbMetaData = res.getMetaData();
		
		// create a mapping of MonetDB columns and related column indexes
		HashMap<String, Integer> colMapping = new HashMap<String, Integer>();
		for(int i=1; i <= monetDbMetaData.getColumnCount(); i++) {
			String colName = monetDbMetaData.getColumnName(i);
			colMapping.put(prepareMonetDbIdentifier(colName), i);
		}
		
		// loop through columns of MS SQL and verify with columns in MonetDB
		for(int i=1; i <= metaData.getColumnCount(); i++) {
			String colName = prepareMonetDbIdentifier(metaData.getColumnName(i));
			
			// col name exists in MonetDB?
			if (colMapping.containsKey(colName)) {
				// verify type
				// TODO: actually verify type
			} else {
				// create column in MonetDB
				log.info("Column " + colName + " is missing in MonetDB table");
				log.info("Adding column " + colName + " in table " + table.getToTableSql() + " in MonetDB...");
				
				String sql = "ALTER TABLE " + table.getToTableSql() + " ADD COLUMN " + createColumnSql(i, metaData);
				Statement createColumn = monetDbConn.createStatement();
				createColumn.execute(sql);
				
				log.info("Column added");
			}
		}		
		
		// close objects
		res.close();
		q.close();
		
		log.info("Table verified");
	}
	
	protected String createColumnSql(int colIndex, ResultSetMetaData metaData) throws SQLException {
		StringBuilder createSql = new StringBuilder();
		
		createSql.append(quoteMonetDbIdentifier(metaData.getColumnName(colIndex).toLowerCase()));
		createSql.append(" ");
		
		HashMap<Integer, String> sqlTypes = new HashMap<Integer, String>();
		sqlTypes.put(Types.BIGINT, "bigint");
		sqlTypes.put(Types.BLOB, "blob");
		sqlTypes.put(Types.BOOLEAN, "boolean");
		sqlTypes.put(Types.CHAR, "char");
		sqlTypes.put(Types.CLOB, "clob");
		sqlTypes.put(Types.DATE, "date");
		sqlTypes.put(Types.DECIMAL, "decimal");
		sqlTypes.put(Types.DOUBLE, "double");
		sqlTypes.put(Types.FLOAT, "float");
		sqlTypes.put(Types.INTEGER, "int");
		sqlTypes.put(Types.NCHAR, "char");
		sqlTypes.put(Types.NCLOB, "clob");
		sqlTypes.put(Types.NUMERIC, "numeric");
		sqlTypes.put(Types.NVARCHAR, "varchar");
		sqlTypes.put(Types.REAL,"real");
		sqlTypes.put(Types.SMALLINT, "smallint");
		sqlTypes.put(Types.TIME,"time");
		sqlTypes.put(Types.TIMESTAMP, "timestamp");
		sqlTypes.put(Types.TINYINT, "tinyint");
		sqlTypes.put(Types.VARCHAR, "varchar");
		
		int colType = metaData.getColumnType(colIndex);
		String colTypeName = null;
		if (sqlTypes.containsKey(colType)) {
			colTypeName = sqlTypes.get(colType);
		}
		
		if (colTypeName == null) {
			throw new SQLException("Unknown SQL type " + colType + " (" + metaData.getColumnTypeName(colIndex) + ")");
		}
		
		
		int precision = metaData.getPrecision(colIndex);
		int scale = metaData.getScale(colIndex);
		
		// fix for numeric/decimal columns with no actual decimals (i.e. numeric(19,0)) 
		if ((colTypeName.equals("decimal") || colTypeName.equals("numeric")) && scale == 0) {
			if (precision <= 2) {
				colTypeName = "tinyint";
			} else if (precision <= 4) {
				colTypeName = "smallint";
			} else if (precision <= 9) {
				colTypeName = "int";
			} else {
				colTypeName = "bigint";
			}
		};		
		
		createSql.append(colTypeName);
		
		// some types required additional info
		if (colTypeName.equals("char") || colTypeName.equals("character")
				|| colTypeName.equals("varchar")
				|| colTypeName.equals("character varying")) {
			createSql.append(" (" + metaData.getColumnDisplaySize(colIndex)
					+ ")");
		} else if (colTypeName.equals("decimal")
				|| colTypeName.equals("numeric")) {
			// MonetDB doesn't support a precision higher than 18
			if (precision > 18)
				precision = 18;

			createSql.append(" (" + precision + ", " + scale + ")");
		} else if (colTypeName.equals("double")) {
			createSql.append("[" + metaData.getPrecision(colIndex) + "]");
		}
		
		createSql.append(" ");
		
		if (metaData.isAutoIncrement(colIndex)) {
			createSql.append("auto_increment ");
		}
		
		if (metaData.isNullable(colIndex) == ResultSetMetaData.columnNoNulls) {
			createSql.append("NOT NULL ");
		}		
		
		return createSql.toString();
	}
	
	protected void truncateTable(CopyTable table) throws SQLException {
		log.info("Truncating table " + table.getToTableSql()  + " on MonetDB server...");
		
		Statement truncateStmt = monetDbConn.createStatement();
		truncateStmt.execute("DELETE FROM " + table.getToTableSql());
		
		log.info("Table truncated");
	}
	
	protected void copyData(CopyTable table, ResultSet resultSet, ResultSetMetaData metaData, long rowCount) throws SQLException {
		log.info("Copying data to table " + table.getToTableSql() + "...");
		
		// build insert SQL
		StringBuilder insertSql = new StringBuilder("INSERT INTO ");
		insertSql.append(table.getToTableSql());
		insertSql.append(" (");
		
		String[] colNames = new String[metaData.getColumnCount()];
		String[] values = new String[metaData.getColumnCount()];
				
		for(int i=1; i <= metaData.getColumnCount(); i++) {
			String colName = metaData.getColumnName(i).toLowerCase();
			colNames[i-1] = quoteMonetDbIdentifier(colName);
		}
		
		insertSql.append(StringUtils.join(colNames, ","));		
		insertSql.append(")");
		insertSql.append(" VALUES (");

		Statement insertStmt = monetDbConn.createStatement();
		
		DecimalFormat formatPerc = new DecimalFormat("#.#");
		
		monetDbConn.setAutoCommit(false);
		
		int batchCount = 0;
		long insertCount = 0;
		while(resultSet.next()) {			
			for(int i=1; i <= metaData.getColumnCount(); i++) {
				Object value = resultSet.getObject(i);
				
				if (value == null) {
					values[i-1] = "NULL";
				} else if (value instanceof Number) {
					values[i-1] = value.toString();
					
					// empty value is unacceptable here, replace with NULL
					if (StringUtils.isEmpty(values[i-1])) {
						values[i-1] = "NULL";
					}
				} else if (value instanceof String) {
					values[i-1] = quoteMonetDbValue((String)value);
				} else {
					throw new SQLException("Unknown value type: " + value.getClass().getName());
				}
			}
			
			StringBuilder insertRecordSql = new StringBuilder(insertSql);
			insertRecordSql.append(StringUtils.join(values, ","));
			insertRecordSql.append(")");
					
			insertStmt.addBatch(insertRecordSql.toString());
			batchCount++;
			
			if (batchCount % this.getBatchSize() == 0) {
				log.info("Inserting next batch of " + this.getBatchSize() + " records...");
				
				insertStmt.executeBatch();
				monetDbConn.commit();
				
				insertStmt.clearBatch();
				insertCount = insertCount + batchCount;
				batchCount = 0;
				
				log.info("Batch inserted");
				float perc = ((float)insertCount / (float)rowCount) * 100;
				log.info("Progress: " + insertCount + " out of " + rowCount + " (" + formatPerc.format(perc) + "%)");
			}
		}
		
		if (batchCount > 0) {
			log.info("Inserting final batch of " + batchCount + " records...");
			
			insertStmt.executeBatch();
			monetDbConn.commit();
			
			insertStmt.clearBatch();
			insertCount = insertCount + batchCount;
			
			log.info("Batch inserted");
			float perc = ((float)insertCount / (float)rowCount) * 100;
			log.info("Progress: " + insertCount + " out of " + rowCount + " (" + formatPerc.format(perc) + "%)");
		}
		
		
		monetDbConn.setAutoCommit(true);
		
		log.info("Finished copying data");			
	}
		
	protected void validateConfig () {	
		boolean isMissing = false;
		
		for (CONFIG_KEYS key : CONFIG_KEYS.values()) {
			String value = config.getProperty(key.toString());
			if (key.isRequired() && StringUtils.isEmpty(value)) {
				isMissing = true;
				log.error("Missing config property: " + key);
			}			
		}
		
		if (isMissing) {
			log.fatal("Missing essential config properties");
			System.exit(1);
		}
		
		// check if batch size has been specified
		String batchSizeStr = config.getProperty(CONFIG_KEYS.BATCH_SIZE.toString());
		if (StringUtils.isEmpty(batchSizeStr) == false) {
			try {
				this.batchSize = Integer.parseInt(batchSizeStr);
			} catch (NumberFormatException e) {
				// don't care, just ignore
			}
		}
	}
	
	protected void findTablesToCopy () {
		for (Entry<Object, Object> entry : config.entrySet()) {
			String propName = entry.getKey().toString().toLowerCase();
			String propValue = entry.getValue().toString();
			boolean boolValue = (propValue.equalsIgnoreCase("true") || propValue.equalsIgnoreCase("yes"));
			
			String[] split = propName.split("\\.");

			if (split.length != 3) continue;	
			
			if (split[0].equals("table") == false) continue;
			
			String id = split[1];
			String key = split[2].toLowerCase();
			
			if (tablesToCopy.containsKey(id) == false) {
				tablesToCopy.put(id,  new CopyTable());
			}
			
			CopyTable table = tablesToCopy.get(id);
			
			if (key.equals("from")) {
				table.setFromName(propValue);
			} else if (key.equals("to")) {
				table.setToName(propValue.toLowerCase());
			} else if (key.equals("create")) {
				table.setCreate(boolValue);
			} else if (key.equals("truncate")) {
				table.setTruncate(boolValue);
			} else if (key.equals("schema")) {
				table.setSchema(propValue);
			} else if (key.equals("drop")) {
				table.setDrop(boolValue);
			}

		}
		
		// verify each specified has a from and to name
		Iterator<Entry<String, CopyTable>> iter = tablesToCopy.entrySet().iterator();
		while(iter.hasNext()) {
			Entry<String, CopyTable> entry = iter.next();
			String id = entry.getKey();
			CopyTable table = entry.getValue();
			
			if (StringUtils.isEmpty(table.getFromName())) {
				log.error("Configuration for '" + id + "' is missing name of from table");
				iter.remove();
				continue;
			}
			
			if (StringUtils.isEmpty(table.getToName())) {
				log.warn("Configuration for '" + id + "' is missing name of to table. Using name of from table (" + table.getFromName() + ")");
				table.setToName(table.getFromName());
			}
		}
		
		if (tablesToCopy.size() == 0) {
			log.error("Configuration has specified NO tables to copy!");
		} else {		
			log.info("The following tables will be copied: ");
			for(CopyTable table : tablesToCopy.values()) {
				log.info("* " + table.getFromName() + " -> " + table.getToName());
			}
		}
	}
	
	protected void openConnections () {
		// make sure JDBC drivers are loaded
		try {
			Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		} catch (ClassNotFoundException e) {
			log.fatal("Unable to load MonetDB JDBC driver");
			System.exit(1);
		}
		
		try {
			Class.forName("net.sourceforge.jtds.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			log.fatal("Unable to load MS SQL jTDS JDBC driver");
			System.exit(1);
		}
		
		try {
			if (mssqlConn == null || mssqlConn.isClosed()) {
				Properties connProps = new Properties();
				String user = config.getProperty(CONFIG_KEYS.MSSQL_USER.toString());
				String password = config.getProperty(CONFIG_KEYS.MSSQL_PASSWORD.toString());
				
				if (StringUtils.isEmpty(user) == false && StringUtils.isEmpty(password) == false) {
					connProps.setProperty("user",  user);
					connProps.setProperty("password", password);
				}
				
				String url = "jdbc:jtds:sqlserver://" + 
						config.getProperty(CONFIG_KEYS.MSSQL_SERVER.toString()) + 
						"/" + config.getProperty(CONFIG_KEYS.MSSQL_DATABASE.toString());
				log.info("Using connection URL for MS SQL Server: " + url);
				
				mssqlConn = DriverManager.getConnection(url, connProps);
				log.info("Opened connection to MS SQL Server");
			}
		} catch (SQLException e) {
			log.fatal("Unable to open connection to MS SQL server", e);
			System.exit(1);
		}
		
		try {
			if (monetDbConn == null || monetDbConn.isClosed()) {
				Properties connProps = new Properties();
				String user = config.getProperty(CONFIG_KEYS.MONETDB_USER.toString());
				String password = config.getProperty(CONFIG_KEYS.MONETDB_PASSWORD.toString());
				
				if (StringUtils.isEmpty(user) == false && StringUtils.isEmpty(password) == false) {
					connProps.setProperty("user",  user);
					connProps.setProperty("password", password);
				}
				
				String url = "jdbc:monetdb://" + 
						config.getProperty(CONFIG_KEYS.MONETDB_SERVER.toString()) + 
						"/" + config.getProperty(CONFIG_KEYS.MONETDB_DATABASE.toString());
				log.info("Using connection URL for MonetDB Server: " + url);
				
				monetDbConn = DriverManager.getConnection(url, connProps);
				log.info("Opened connection to MonetDB Server");
			}
		} catch (SQLException e) {
			log.fatal("Unable to open connection to MonetDB server", e);
			closeConnections();
			System.exit(1);
		}
	}
	
	protected void closeConnections () {
		log.info("Closing database connections...");
		
		try {
			if (mssqlConn != null && mssqlConn.isClosed() == false) {
				mssqlConn.close();
				log.info("Closed connection to MS SQL server");
			}
		} catch (SQLException e) {
			// don't care about this exception
			log.warn("Unable to close connection to MS SQL server", e);
		}
		
		try {
			if (monetDbConn != null && monetDbConn.isClosed() == false) {
				monetDbConn.close();
				log.info("Closed connection to MonetDB server");
			}
		} catch (SQLException e) {
			// don't care about this exception
			log.warn("Unable to close connection to MonetDB server", e);
		}		
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		log.info("Started MSSQL2MonetDB copy tool");
				
		PropertyConfigurator.configure("log4j.properties");
				
		Options options = new Options();
		
		OptionBuilder.hasArg(true);
		OptionBuilder.isRequired(true);
		OptionBuilder.withDescription("Specify the configuration properties file");
		OptionBuilder.withLongOpt("config");
		options.addOption(OptionBuilder.create("c"));
		
		CommandLineParser parser = new BasicParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse( options, args);
		} catch (ParseException e) {
			System.err.println("ERROR: " + e.getMessage());
			System.out.println("");
			
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("mssql2monetdb", options);
			
			System.exit(1);
		}
				
		File configFile = new File(cmd.getOptionValue("config"));
		log.info("Using config file: " + configFile.getAbsolutePath());
		
		if (configFile.exists() == false || configFile.canRead() == false) {
			
		}
		
		Properties config = new Properties();
		try {
			config.load(new FileInputStream(configFile));
		} catch (Exception e) {
			System.err.println("ERROR: unable to read config file");
			e.printStackTrace();
			System.exit(1);
		}
		
		// run tool
		(new CopyTool(config)).run();
	}

}
