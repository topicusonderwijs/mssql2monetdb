package nl.topicus.mssql2monetdb;

import java.io.File;
import java.io.FileInputStream;
import java.sql.*;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import nl.cwi.monetdb.mcl.io.BufferedMCLReader;
import nl.cwi.monetdb.mcl.io.BufferedMCLWriter;
import nl.cwi.monetdb.mcl.net.MapiSocket;

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

public class CopyTool
{
	private static final Logger LOG = Logger.getLogger(CopyTool.class);

	public static final int DEFAULT_BATCH_SIZE = 10000;

	private Properties config;

	private Connection mssqlConn;

	private Connection monetDbConn;

	private MapiSocket monetDbServer;

	private HashMap<String, CopyTable> tablesToCopy = new HashMap<String, CopyTable>();

	private int batchSize = DEFAULT_BATCH_SIZE;

	private DecimalFormat formatPerc = new DecimalFormat("#.#");

	/**
	 * @param args
	 */
	public static void main(String[] args)
	{
		LOG.info("Started MSSQL2MonetDB copy tool");

		PropertyConfigurator.configure("log4j.properties");

		Options options = new Options();

		OptionBuilder.hasArg(true);
		OptionBuilder.isRequired(true);
		OptionBuilder.withDescription("Specify the configuration properties file");
		OptionBuilder.withLongOpt("config");
		options.addOption(OptionBuilder.create("c"));

		CommandLineParser parser = new BasicParser();
		CommandLine cmd = null;
		try
		{
			cmd = parser.parse(options, args);
		}
		catch (ParseException e)
		{
			LOG.error("ERROR: " + e.getMessage());

			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("mssql2monetdb", options);

			System.exit(1);
		}
		if (cmd == null)
		{
			LOG.error("CommandLine parser is null");
			return;
		}

		File configFile = new File(cmd.getOptionValue("config"));
		LOG.info("Using config file: " + configFile.getAbsolutePath());

		Properties config = new Properties();
		try
		{
			config.load(new FileInputStream(configFile));
		}
		catch (Exception e)
		{
			LOG.error("ERROR: unable to read config file");
			e.printStackTrace();
			System.exit(1);
		}

		// run tool
		(new CopyTool(config)).run();
	}

	public CopyTool(Properties config)
	{
		this.config = config;
		this.validateConfig(config);
		this.tablesToCopy = findTablesToCopy(config);
	}

	public static String prepareMonetDbIdentifier(String ident)
	{
		// MonetDB only supports lowercase identifiers
		ident = ident.toLowerCase();

		// MonetDB doesn't support any special characters so replace with underscore
		ident = ident.replaceAll("[^a-zA-Z0-9]+", "_");

		return ident;
	}

	public static String quoteMonetDbValue(String value)
	{
		return "'" + value.replaceAll("\\\\", "\\\\\\\\").replaceAll("'", "\\\\'") + "'";
	}

	public static String quoteMonetDbIdentifier(String ident)
	{
		// prepare identifier
		ident = prepareMonetDbIdentifier(ident);

		// make sure identifier is actually quoted
		ident = "\"" + ident + "\"";

		return ident;
	}

	public void run()
	{
		if (tablesToCopy.size() > 0)
		{
			this.openConnections();

			for (CopyTable table : tablesToCopy.values())
			{
				try
				{
					// backup table if configured
					if (table.isBackup())
						backupTable(table.getResultTable());

					// copy new data to monetdb
					copyTable(table);
				}
				catch (SQLException e)
				{
					LOG.error("Unable to copy data from table '" + table.getFromName() + "'", e);
				}
			}

			// if there are any temp table copies configured, then copy the temp tables to
			// result tables. We do this after the rest is done to reduce down-time
			this.copyTempTableToResultTable();

			this.closeConnections();
		}

		LOG.info("Finished!");
	}

	protected void openConnections()
	{
		// make sure JDBC drivers are loaded
		try
		{
			Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		}
		catch (ClassNotFoundException e)
		{
			LOG.fatal("Unable to load MonetDB JDBC driver");
			System.exit(1);
		}

		try
		{
			Class.forName("net.sourceforge.jtds.jdbc.Driver");
		}
		catch (ClassNotFoundException e)
		{
			LOG.fatal("Unable to load MS SQL jTDS JDBC driver");
			System.exit(1);
		}

		try
		{
			if (mssqlConn == null || mssqlConn.isClosed())
			{
				Properties connProps = new Properties();
				String user = config.getProperty(CONFIG_KEYS.MSSQL_USER.toString());
				String password = config.getProperty(CONFIG_KEYS.MSSQL_PASSWORD.toString());
				String instance = config.getProperty(CONFIG_KEYS.MSSQL_INSTANCE.toString());

				if (StringUtils.isEmpty(user) == false && StringUtils.isEmpty(password) == false)
				{
					connProps.setProperty("user", user);
					connProps.setProperty("password", password);
				}

				if (StringUtils.isEmpty(instance) == false)
				{
					connProps.setProperty("instance", instance);
				}

				String url =
					"jdbc:jtds:sqlserver://"
						+ config.getProperty(CONFIG_KEYS.MSSQL_SERVER.toString()) + "/"
						+ config.getProperty(CONFIG_KEYS.MSSQL_DATABASE.toString());
				LOG.info("Using connection URL for MS SQL Server: " + url);

				mssqlConn = DriverManager.getConnection(url, connProps);
				LOG.info("Opened connection to MS SQL Server");
			}
		}
		catch (SQLException e)
		{
			LOG.fatal("Unable to open connection to MS SQL server", e);
			System.exit(1);
		}

		try
		{
			if (monetDbConn == null || monetDbConn.isClosed())
			{
				Properties connProps = new Properties();
				String user = config.getProperty(CONFIG_KEYS.MONETDB_USER.toString());
				String password = config.getProperty(CONFIG_KEYS.MONETDB_PASSWORD.toString());

				if (StringUtils.isEmpty(user) == false && StringUtils.isEmpty(password) == false)
				{
					connProps.setProperty("user", user);
					connProps.setProperty("password", password);
				}

				String url =
					"jdbc:monetdb://" + config.getProperty(CONFIG_KEYS.MONETDB_SERVER.toString())
						+ "/" + config.getProperty(CONFIG_KEYS.MONETDB_DATABASE.toString());
				LOG.info("Using connection URL for MonetDB Server: " + url);

				monetDbConn = DriverManager.getConnection(url, connProps);
				LOG.info("Opened connection to MonetDB Server");
			}
		}
		catch (SQLException e)
		{
			LOG.fatal("Unable to open connection to MonetDB server", e);
			closeConnections();
			System.exit(1);
		}

		monetDbServer = new MapiSocket();

		monetDbServer.setDatabase(config.getProperty(CONFIG_KEYS.MONETDB_DATABASE.toString()));
		monetDbServer.setLanguage("sql");

		try
		{
			LOG.info("Opening direct connection to MonetDB server...");
			List<String> warnList =
				monetDbServer.connect(config.getProperty(CONFIG_KEYS.MONETDB_SERVER.toString()),
					50000, config.getProperty(CONFIG_KEYS.MONETDB_USER.toString()),
					config.getProperty(CONFIG_KEYS.MONETDB_PASSWORD.toString()));

			if (warnList != null && warnList.size() > 0)
			{
				for (String warning : warnList)
				{
					LOG.error(warning);
				}

				LOG.error("Unable to setup direct connection with MonetDB server");
				monetDbServer.close();
				monetDbServer = null;

			}
			else
			{
				LOG.info("Direct connection opened");
			}
		}
		catch (Exception e)
		{
			LOG.error("Unable to setup direct connection with MonetDB server");

			monetDbServer.close();
			monetDbServer = null;
		}

	}

	protected HashMap<String, CopyTable> findTablesToCopy(Properties config)
	{
		HashMap<String, CopyTable> tablesToCopy = new HashMap<String, CopyTable>();
		for (Entry<Object, Object> entry : config.entrySet())
		{
			String propName = entry.getKey().toString().toLowerCase();
			String propValue = entry.getValue().toString();
			boolean boolValue =
				(propValue.equalsIgnoreCase("true") || propValue.equalsIgnoreCase("yes"));

			String[] split = propName.split("\\.");

			if (split.length != 3)
				continue;

			if (split[0].equals("table") == false)
				continue;

			String id = split[1];
			String key = split[2].toLowerCase();

			CopyTable table = tablesToCopy.get(id);
			// if table does not exist than add new CopyTable with a MonetDBTable
			if (table == null)
			{
				table = new CopyTable();
				table.getMonetDBTables().add(new MonetDBTable(table));
			}

			if (key.equals("from"))
			{
				table.setFromName(propValue);
			}
			else if (key.equals("to"))
			{
				table.getResultTable().setName(propValue.toLowerCase());
			}
			else if (key.equals("schema"))
			{
				table.getResultTable().setSchema(propValue);
			}
			else if (key.equals("create"))
			{
				table.setCreate(boolValue);
			}
			else if (key.equals("truncate"))
			{
				table.setTruncate(boolValue);
			}
			else if (key.equals("drop"))
			{
				table.setDrop(boolValue);
			}
			else if (key.equals("copyviatemptable"))
			{
				table.setCopyViaTempTable(boolValue);
			}
			else if (key.equals("temptableprefix"))
			{
				table.setTempTablePrefix(propValue);
			}
			else if (key.equals("backup"))
			{
				table.setBackup(boolValue);
			}

			tablesToCopy.put(id, table);
		}

		// verify each specified has a from and to name and add temp tables
		// and add temptable configuration if copyViaTempTable
		Iterator<Entry<String, CopyTable>> iter = tablesToCopy.entrySet().iterator();
		while (iter.hasNext())
		{
			Entry<String, CopyTable> entry = iter.next();
			String id = entry.getKey();
			CopyTable table = entry.getValue();
			if (table.getResultTable() == null)
			{
				LOG.error("Configuration for '" + id + "' is missing a result table");
				iter.remove();
				continue;
			}

			if (StringUtils.isEmpty(table.getFromName()))
			{
				LOG.error("Configuration for '" + id + "' is missing name of from table");
				iter.remove();
				continue;
			}

			if (StringUtils.isEmpty(table.getResultTable().getName()))
			{
				LOG.warn("Configuration for '" + id
					+ "' is missing name of to table. Using name of from table ("
					+ table.getFromName() + ")");
				table.getResultTable().setName(table.getFromName());
			}

			if (table.isCopyViaTempTable() && table.getTempTable() == null)
			{
				MonetDBTable tempTable = new MonetDBTable(table);
				tempTable.setTempTable(true);
				// toName = tempTablePrefix + toName
				tempTable.setName(table.getTempTablePrefix() + table.getResultTable().getName());
				table.getMonetDBTables().add(tempTable);
			}
		}

		if (tablesToCopy.size() == 0)
		{
			LOG.error("Configuration has specified NO tables to copy!");
		}
		else
		{
			LOG.info("The following tables will be copied: ");
			for (CopyTable table : tablesToCopy.values())
			{
				LOG.info("* " + table.getFromName() + " -> " + table.getResultTable().getName());
			}
		}

		return tablesToCopy;
	}

	/**
	 * Copy a MSSQL table to MonetDB. This will copy the MSSQL data to the result table or
	 * a temporary table if configured that way. This includes auto-creating the necessary
	 * tables and importing the data using selects or copy into.
	 */
	protected void copyTable(CopyTable table) throws SQLException
	{
		LOG.info("Starting with copy of table " + table.getFromName() + "...");

		// select data from MS SQL Server
		Statement selectStmt = mssqlConn.createStatement();

		// get number of rows in table
		ResultSet resultSet =
			selectStmt.executeQuery("SELECT COUNT(*) FROM [" + table.getFromName() + "]");
		resultSet.next();

		long rowCount = resultSet.getLong(1);
		LOG.info("Found " + rowCount + " rows in table " + table.getFromName());

		resultSet.close();

		// get all data from table
		resultSet = selectStmt.executeQuery("SELECT * FROM [" + table.getFromName() + "]");

		// get meta data (column info and such)
		ResultSetMetaData metaData = resultSet.getMetaData();

		MonetDBTable copyToTable =
			table.isCopyViaTempTable() ? table.getTempTable() : table.getResultTable();

		// check tables in monetdb
		checkTableInMonetDb(copyToTable, metaData);

		// do truncate?
		if (table.truncate())
		{
			truncateMonetDBTable(copyToTable);
		}

		// copy data
		if (table.getCopyMethod() == CopyTable.COPY_METHOD_COPYINTO && monetDbServer != null)
		{
			try
			{
				copyDataWithCopyInto(copyToTable, resultSet, metaData, rowCount);
			}
			catch (Exception e)
			{
				LOG.error("Copying data failed", e);
			}
		}
		else
		{
			try
			{
				copyData(copyToTable, resultSet, metaData, rowCount);
			}
			catch (SQLException e)
			{
				LOG.error("Copying data failed", e);

				// print full chain of exceptions
				SQLException nextException = e.getNextException();
				while (nextException != null)
				{
					nextException.printStackTrace();
					nextException = nextException.getNextException();
				}
			}
		}

		// close everything again
		resultSet.close();
		selectStmt.close();

		LOG.info("Finished copy of table " + table.getFromName());
	}

	/**
	 * Backup a table by copying the data into a backup table named backup_resultTable by
	 * resultTable and metaData of the MSSQL table that is copied.
	 * 
	 * @throws SQLException
	 */
	protected void backupTable(MonetDBTable resultTable) throws SQLException
	{
		// create a MonetDBTable object for the backup table
		MonetDBTable backupTable = new MonetDBTable(resultTable.getCopyTable());
		backupTable.setSchema(resultTable.getSchema());
		backupTable.setName("backup_" + resultTable.getName());
		// drop and recreate backup table regardless if it exists
		dropMonetDBTable(backupTable);
		// copy from resulttable to backup table
		copyMonetDBTableToNewMonetDBTable(resultTable, backupTable);
	}

	protected void checkTableInMonetDb(MonetDBTable monetDBTable, ResultSetMetaData metaData)
			throws SQLException
	{
		boolean tableExists = monetDBTableExists(monetDBTable);
		// can't auto create?
		if (tableExists == false && monetDBTable.getCopyTable().create() == false)
		{
			throw new SQLException("Table " + monetDBTable.getToTableSql()
				+ " does not exist in MonetDB database and auto-create is set to false");
		}

		// need to drop?
		if (tableExists && monetDBTable.getCopyTable().drop())
		{
			dropMonetDBTable(monetDBTable);
			tableExists = false;
		}

		if (tableExists)
		{
			// verify table is as expected
			this.verifyExistingTable(monetDBTable, metaData);
		}
		else
		{
			createMonetDBTable(monetDBTable, metaData);
		}
	}

	protected void createMonetDBTable(MonetDBTable monetDBTable, ResultSetMetaData metaData)
			throws SQLException
	{
		// build SQL query to create table
		LOG.info("Creating table " + monetDBTable.getToTableSql() + " on MonetDB server...");
		StringBuilder createSql =
			new StringBuilder("CREATE TABLE " + monetDBTable.getToTableSql() + " (");

		for (int i = 1; i <= metaData.getColumnCount(); i++)
		{
			createSql.append(createColumnSql(i, metaData));
			createSql.append(",");
		}
		createSql.deleteCharAt(createSql.length() - 1);
		createSql.append(")");

		// execute CREATE TABLE SQL query
		monetDbConn.createStatement().execute(createSql.toString());
		LOG.info("Table created");

		// fresh table so we can use COPY INTO since we know its ok
		monetDBTable.getCopyTable().setCopyMethod(CopyTable.COPY_METHOD_COPYINTO);

	}

	protected void copyTempTableToResultTable()
	{
		// create table schema.temptable as select * from schema.resulttable with data;
		LOG.info("Copying the temp table to the result table");
		try
		{
			for (CopyTable copyTable : tablesToCopy.values())
			{
				if (copyTable.isCopyViaTempTable())
				{
					// drop result table before replacing with temp table
					if (monetDBTableExists(copyTable.getResultTable()))
					{
						dropMonetDBTable(copyTable.getResultTable());
					}
					copyMonetDBTableToNewMonetDBTable(copyTable.getTempTable(),
						copyTable.getResultTable());
					// drop temp table, we wont need it anymore
					dropMonetDBTable(copyTable.getTempTable());
				}
			}
		}
		catch (SQLException e)
		{
			LOG.error("Error copying temp table to result tabel", e);
		}
		LOG.info("Finished copying the temp table to the result table");
	}

	protected void verifyExistingTable(MonetDBTable table, ResultSetMetaData metaData)
			throws SQLException
	{
		LOG.info("Verifying existing table " + table.getToTableSql()
			+ " in MonetDB matches table schema in MS SQL...");

		// do a select on the table in MonetDB to get its metadata
		Statement q = monetDbConn.createStatement();
		ResultSet res = q.executeQuery("SELECT * FROM " + table.getToTableSql() + " LIMIT 1");
		ResultSetMetaData monetDbMetaData = res.getMetaData();

		// create a mapping of MonetDB columns and related column indexes
		HashMap<String, Integer> colMapping = new HashMap<String, Integer>();
		for (int i = 1; i <= monetDbMetaData.getColumnCount(); i++)
		{
			String colName = monetDbMetaData.getColumnName(i);
			colMapping.put(prepareMonetDbIdentifier(colName), i);
		}

		// loop through columns of MS SQL and verify with columns in MonetDB
		for (int i = 1; i <= metaData.getColumnCount(); i++)
		{
			String colName = prepareMonetDbIdentifier(metaData.getColumnName(i));

			// col name exists in MonetDB?
			if (colMapping.containsKey(colName))
			{
				// verify type
				// TODO: actually verify type
			}
			else
			{
				// create column in MonetDB
				LOG.info("Column " + colName + " is missing in MonetDB table");
				LOG.info("Adding column " + colName + " in table " + table.getToTableSql()
					+ " in MonetDB...");

				String sql =
					"ALTER TABLE " + table.getToTableSql() + " ADD COLUMN "
						+ createColumnSql(i, metaData);
				Statement createColumn = monetDbConn.createStatement();
				createColumn.execute(sql);

				LOG.info("Column added");
			}
		}

		// close objects
		res.close();
		q.close();

		LOG.info("Table verified");
	}

	protected String createColumnSql(int colIndex, ResultSetMetaData metaData) throws SQLException
	{
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
		sqlTypes.put(Types.REAL, "real");
		sqlTypes.put(Types.SMALLINT, "smallint");
		sqlTypes.put(Types.TIME, "time");
		sqlTypes.put(Types.TIMESTAMP, "timestamp");
		sqlTypes.put(Types.TINYINT, "tinyint");
		sqlTypes.put(Types.VARCHAR, "varchar");

		int colType = metaData.getColumnType(colIndex);
		String colTypeName = null;
		if (sqlTypes.containsKey(colType))
		{
			colTypeName = sqlTypes.get(colType);
		}

		if (colTypeName == null)
		{
			throw new SQLException("Unknown SQL type " + colType + " ("
				+ metaData.getColumnTypeName(colIndex) + ")");
		}

		int precision = metaData.getPrecision(colIndex);
		int scale = metaData.getScale(colIndex);

		// fix for numeric/decimal columns with no actual decimals (i.e. numeric(19,0))
		if ((colTypeName.equals("decimal") || colTypeName.equals("numeric")) && scale == 0)
		{
			if (precision <= 2)
			{
				colTypeName = "tinyint";
			}
			else if (precision <= 4)
			{
				colTypeName = "smallint";
			}
			else if (precision <= 9)
			{
				colTypeName = "int";
			}
			else
			{
				colTypeName = "bigint";
			}
		}

		createSql.append(colTypeName);

		// some types required additional info
		if (colTypeName.equals("char") || colTypeName.equals("character")
			|| colTypeName.equals("varchar") || colTypeName.equals("character varying"))
		{
			createSql.append(" (" + metaData.getColumnDisplaySize(colIndex) + ")");
		}
		else if (colTypeName.equals("decimal") || colTypeName.equals("numeric"))
		{
			// MonetDB doesn't support a precision higher than 18
			if (precision > 18)
				precision = 18;

			createSql.append(" (" + precision + ", " + scale + ")");
		}

		createSql.append(" ");

		if (metaData.isAutoIncrement(colIndex))
		{
			createSql.append("auto_increment ");
		}

		if (metaData.isNullable(colIndex) == ResultSetMetaData.columnNoNulls)
		{
			createSql.append("NOT NULL ");
		}

		return createSql.toString();
	}

	protected void copyDataWithCopyInto(MonetDBTable monetDBTable, ResultSet resultSet,
			ResultSetMetaData metaData, long rowCount) throws Exception
	{
		LOG.info("Using COPY INTO to copy data to table " + monetDBTable.getToTableSql() + "...");

		BufferedMCLReader in = monetDbServer.getReader();
		BufferedMCLWriter out = monetDbServer.getWriter();

		String error = in.waitForPrompt();
		if (error != null)
			throw new Exception(error);

		String query =
			"COPY INTO " + monetDBTable.getToTableSql()
				+ " FROM STDIN USING DELIMITERS ',','\\n','\"' NULL AS '';";

		// the leading 's' is essential, since it is a protocol
		// marker that should not be omitted, likewise the
		// trailing semicolon
		out.write('s');
		out.write(query);
		out.newLine();

		long startTime = System.currentTimeMillis();
		long insertCount = 0;
		int columnCount = metaData.getColumnCount();

		while (resultSet.next())
		{
			for (int i = 1; i <= columnCount; i++)
			{
				Object value = resultSet.getObject(i);
				String valueStr = "";

				if (value == null)
				{
					valueStr = "";
				}
				else
				{
					valueStr = value.toString();

					// escape \ with \\
					valueStr = valueStr.replaceAll("\\\\", "\\\\\\\\");

					// escape " with \"
					valueStr = valueStr.replaceAll("\"", "\\\\\"");
				}

				out.write("\"" + valueStr + "\"");

				// column separator (not for last column)
				if (i < columnCount)
				{
					out.write(",");
				}
			}

			// record separator
			out.newLine();

			insertCount++;

			if (insertCount % 100000 == 0)
			{
				printInsertProgress(startTime, insertCount, rowCount);
			}
		}
		printInsertProgress(startTime, insertCount, rowCount);

		LOG.info("Finalising COPY INTO... this may take a while!");

		out.writeLine("");

		error = in.waitForPrompt();
		if (error != null)
			throw new Exception(error);

		out.writeLine(""); // server wants more, we're going to tell it, this is it

		error = in.waitForPrompt();
		if (error != null)
			throw new Exception(error);

		LOG.info("Finished copying data");
	}

	protected void copyData(MonetDBTable monetDBTable, ResultSet resultSet,
			ResultSetMetaData metaData, long rowCount) throws SQLException
	{
		LOG.info("Copying data to table " + monetDBTable.getToTableSql() + "...");

		// build insert SQL
		StringBuilder insertSql = new StringBuilder("INSERT INTO ");
		insertSql.append(monetDBTable.getToTableSql());
		insertSql.append(" (");

		String[] colNames = new String[metaData.getColumnCount()];
		String[] values = new String[metaData.getColumnCount()];

		for (int i = 1; i <= metaData.getColumnCount(); i++)
		{
			String colName = metaData.getColumnName(i).toLowerCase();
			colNames[i - 1] = quoteMonetDbIdentifier(colName);
		}

		insertSql.append(StringUtils.join(colNames, ","));
		insertSql.append(")");
		insertSql.append(" VALUES (");

		Statement insertStmt = monetDbConn.createStatement();

		monetDbConn.setAutoCommit(false);

		long startTime = System.currentTimeMillis();

		int batchCount = 0;
		long insertCount = 0;
		while (resultSet.next())
		{
			for (int i = 1; i <= metaData.getColumnCount(); i++)
			{
				Object value = resultSet.getObject(i);

				if (value == null)
				{
					values[i - 1] = "NULL";
				}
				else if (value instanceof Number)
				{
					values[i - 1] = value.toString();

					// empty value is unacceptable here, replace with NULL
					if (StringUtils.isEmpty(values[i - 1]))
					{
						values[i - 1] = "NULL";
					}
				}
				else if (value instanceof String || value instanceof Timestamp)
				{
					values[i - 1] = quoteMonetDbValue(value.toString());
				}
				else
				{
					throw new SQLException("Unknown value type: " + value.getClass().getName());
				}
			}

			StringBuilder insertRecordSql = new StringBuilder(insertSql);
			insertRecordSql.append(StringUtils.join(values, ","));
			insertRecordSql.append(")");

			insertStmt.addBatch(insertRecordSql.toString());
			batchCount++;

			if (batchCount % batchSize == 0)
			{
				LOG.info("Inserting next batch of " + batchSize + " records...");

				insertStmt.executeBatch();
				monetDbConn.commit();

				insertStmt.clearBatch();
				insertCount = insertCount + batchCount;
				batchCount = 0;

				printInsertProgress(startTime, insertCount, rowCount);
			}
		}

		if (batchCount > 0)
		{
			LOG.info("Inserting final batch of " + batchCount + " records...");

			insertStmt.executeBatch();
			monetDbConn.commit();

			insertStmt.clearBatch();
			insertCount = insertCount + batchCount;

			printInsertProgress(startTime, insertCount, rowCount);
		}

		monetDbConn.setAutoCommit(true);

		LOG.info("Finished copying data");
	}

	protected void printInsertProgress(long startTime, long insertCount, long rowCount)
	{
		long totalTime = System.currentTimeMillis() - startTime;

		// how much time for current inserted records?
		float timePerRecord = (float) (totalTime / 1000) / (float) insertCount;

		long timeLeft = Float.valueOf((rowCount - insertCount) * timePerRecord).longValue();

		LOG.info("Records inserted");
		float perc = ((float) insertCount / (float) rowCount) * 100;
		LOG.info("Progress: " + insertCount + " out of " + rowCount + " ("
			+ formatPerc.format(perc) + "%)");
		LOG.info("Time: " + (totalTime / 1000) + " seconds spent; estimated time left is "
			+ timeLeft + " seconds");
	}

	protected void validateConfig(Properties config)
	{
		boolean isMissing = false;

		for (CONFIG_KEYS key : CONFIG_KEYS.values())
		{
			String value = config.getProperty(key.toString());
			if (key.isRequired() && StringUtils.isEmpty(value))
			{
				isMissing = true;
				LOG.error("Missing config property: " + key);
			}
		}

		if (isMissing)
		{
			LOG.fatal("Missing essential config properties");
			System.exit(1);
		}

		// check if batch size has been specified
		String batchSizeStr = config.getProperty(CONFIG_KEYS.BATCH_SIZE.toString());
		if (StringUtils.isEmpty(batchSizeStr) == false)
		{
			try
			{
				this.batchSize = Integer.parseInt(batchSizeStr);
			}
			catch (NumberFormatException e)
			{
				// don't care, just ignore
			}
		}
	}

	private boolean monetDBTableExists(MonetDBTable monetDBTable) throws SQLException
	{
		boolean tableExists = true;
		try
		{
			Statement q = monetDbConn.createStatement();
			q.executeQuery("SELECT * FROM " + monetDBTable.getToTableSql() + " LIMIT 1");
		}
		catch (SQLException e)
		{
			if (e.getMessage().indexOf("no such table") > -1)
			{
				// ok, so does not exist
				tableExists = false;
			}
			else
			{
				throw e;
			}
		}

		return tableExists;
	}

	private void truncateMonetDBTable(MonetDBTable monetDBTable) throws SQLException
	{
		if (monetDBTableExists(monetDBTable))
		{
			LOG.info("Truncating table " + monetDBTable.getToTableSql() + " on MonetDB server...");
			monetDbConn.createStatement().execute("DELETE FROM " + monetDBTable.getToTableSql());
			LOG.info("Table truncated");
		}
		else
		{
			LOG.warn("Did not truncate " + monetDBTable.getToTableSql()
				+ " because it did not exist.");
		}
	}

	private void dropMonetDBTable(MonetDBTable monetDBTable) throws SQLException
	{
		if (monetDBTableExists(monetDBTable))
		{
			LOG.info("Dropping table " + monetDBTable.getToTableSql() + " in MonetDB database...");
			monetDbConn.createStatement().executeUpdate(
				"DROP TABLE " + monetDBTable.getToTableSql());
			LOG.info("Table " + monetDBTable.getToTableSql() + " dropped");
		}
		else
		{
			LOG.warn("Did not drop " + monetDBTable.getToTableSql() + " because it did not exist.");
		}
	}

	/**
	 * Copy (create and insert data) a {@link MonetDBTable} to another
	 * {@link MonetDBTable} with the following statement: CREATE TABLE newTable AS SELECT
	 * * FROM existingTable WITH DATA;
	 * 
	 * @throws SQLException
	 */
	private void copyMonetDBTableToNewMonetDBTable(MonetDBTable existingTable, MonetDBTable newTable)
			throws SQLException
	{
		if (monetDBTableExists(existingTable))
		{
			LOG.info("Copying table " + existingTable.getToTableSql() + " to "
				+ newTable.getToTableSql() + " with data");
			monetDbConn.createStatement().execute(
				"CREATE TABLE " + newTable.getToTableSql() + " AS SELECT * FROM "
					+ existingTable.getToTableSql() + " WITH DATA");
		}
		else
		{
			LOG.warn("Did not copy " + existingTable.getToTableSql() + " to "
				+ newTable.getToTableSql() + ", because" + existingTable.getToTableSql()
				+ " did not exist.");
		}
	}

	protected void closeConnections()
	{
		LOG.info("Closing database connections...");

		try
		{
			if (mssqlConn != null && mssqlConn.isClosed() == false)
			{
				mssqlConn.close();
				LOG.info("Closed connection to MS SQL server");
			}
		}
		catch (SQLException e)
		{
			// don't care about this exception
			LOG.warn("Unable to close connection to MS SQL server", e);
		}

		try
		{
			if (monetDbConn != null && monetDbConn.isClosed() == false)
			{
				monetDbConn.close();
				LOG.info("Closed JDBC connection to MonetDB server");
			}
		}
		catch (SQLException e)
		{
			// don't care about this exception
			LOG.warn("Unable to close connection to MonetDB server", e);
		}

		if (monetDbServer != null)
		{
			monetDbServer.close();
			LOG.info("Closed direct connection to MonetDB server");
		}
	}

}
