package nl.topicus.mssql2monetdb;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import nl.cwi.monetdb.mcl.io.BufferedMCLReader;
import nl.cwi.monetdb.mcl.io.BufferedMCLWriter;
import nl.topicus.mssql2monetdb.util.EmailUtil;
import nl.topicus.mssql2monetdb.util.MonetDBUtil;
import nl.topicus.mssql2monetdb.util.MssqlUtil;
import nl.topicus.mssql2monetdb.util.SerializableResultSetMetaData;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVReader;

public class CopyTool
{
	private static final Logger LOG =  LoggerFactory.getLogger(CopyTool.class);
	
	private static final int SLEEP_INCREMENT = 1 * 60 * 1000;

	private CopyToolConfig config;

	private DecimalFormat formatPerc = new DecimalFormat("#.#");
	
	private Object lastRunValue;
	
	private int lastRunColType;
	
	private Pattern versionPattern = Pattern.compile("[0-9]{4}_[0-9]{2}_[0-9]{2}_[0-9]{2}_[0-9]{2}_[0-9]{2}$");
	
	private static final int EXIT_CODE_ERROR = 1;
	
	private static final int EXIT_CODE_NO_NEW_DATA = 2;

	/**
	 * @param args
	 */
	public static void main(String[] args)
	{
		//the logger is not yet initialized
		System.out.println("Started MSSQL2MonetDB copy tool");

		CopyToolConfig config = null;
		
		// load config
		try {
			config = new CopyToolConfig(args);
		} catch (Exception e) {
			LOG.error(e.getMessage());
			System.exit(EXIT_CODE_ERROR);
		}
		
		// setup tool
		CopyTool tool = new CopyTool();
		
		// run tool
		try {
			tool.run(config);
			
		} catch (NoNewDataException e) {
			LOG.info(e.getMessage());
			LOG.info("Finished");
			System.exit(EXIT_CODE_NO_NEW_DATA);
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
			EmailUtil.sendMail(e, config.getDatabaseProperties());
			
			CopyToolConnectionManager.getInstance().closeConnections();
			LOG.info("Finished");
			System.exit(EXIT_CODE_ERROR);
		}
	}

	public void run(CopyToolConfig config) throws Exception
	{
		if (config == null)
		{
			throw new CopyToolException("CopyToolConfig cannot be null");
		}
		this.config = config;
		
		// load database drivers
		loadDatabaseDrivers();
		CopyToolConnectionManager.getInstance().setConfig(config);
		
		// how should we run? with scheduler (i.e. infinite) or one-time
		if (config.isSchedulerEnabled()) 
		{
			int interval = config.getSchedulerInterval() * 1000;
			int timeLeft = interval;
			
			while(true)
			{
				// do copy
				try {
					doCopy();
				} catch (Exception e) {
					// we catch every exception because we don't want to fail
					// out of the scheduler
					LOG.error("Caught exception: " + e.getMessage(), e);
					EmailUtil.sendMail(e, this.config.getDatabaseProperties());
					CopyToolConnectionManager.getInstance().closeConnections();
				}

				LOG.info("Scheduling enabled, sleeping for " + (interval/1000) + " seconds until next run");
				timeLeft = interval;
				
				// sleep in increments of 5 minutes
				while(timeLeft > 0)
				{
					// do sleep
					try {
						Thread.sleep((timeLeft > SLEEP_INCREMENT) ? SLEEP_INCREMENT : timeLeft);
					} catch (InterruptedException e) {
						LOG.warn("Scheduled waiting time got interrupted!");
					}
					
					timeLeft = timeLeft - SLEEP_INCREMENT;
					if (timeLeft > 0)
						LOG.info("Still sleeping " + (timeLeft/1000) + " seconds until next run");
				}
				
				LOG.info("Starting next run!");
			}
		}
		else
		{
			// do one-time copy
			doCopy();
		}
	}
	
	private void doCopy () throws Exception
	{
		Map<String, CopyTable> tablesToCopy = config.getTablesToCopy();
		
		boolean switchOnly = config.isSwitchOnly();
		boolean noSwitch = config.hasNoSwitch();
		
		// no tables to copy?
		if (tablesToCopy.size() == 0)
		{
			throw new Exception("No tables to copy");
		}
		
		// check if trigger is enabled and if so, if there is any new data
		boolean anyErrors = false;
		
		if(!switchOnly){
			
			if (config.isTriggerEnabled() && !checkForNewData())
			{
				CopyToolConnectionManager.getInstance().closeConnections();
				throw new NoNewDataException("No indication of new data from trigger source '" + config.getTriggerTable() + "." + config.getTriggerColumn() + "'");
			}
			
			// verify all MSSQL tables have data
			if (!MssqlUtil.allMSSQLTablesHaveData(tablesToCopy))
			{
				LOG.warn("Not all tables have data");
				CopyToolConnectionManager.getInstance().closeConnections();
				return;
			}
			
			// verify MonetDB database is working by opening connection
			CopyToolConnectionManager.getInstance().openMonetDbConnection();
			
			LOG.info("STARTING PHASE 1: copying data from MS SQL source databases to local disk");
			
			// phase 1: copy data from MS SQL sources to local disk
			for(CopyTable table : tablesToCopy.values())
			{
				copyData(table);
			}

			LOG.info("PHASE 1 FINISHED: all data copied from MS SQL source databases to local disk");
			
			LOG.info("STARTING PHASE 2: loading data into target MonetDB database");
			
			// get a SQL-friendly representation of the current date/time of the load
			// used for the fast view switching tables
			DateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss");
			Calendar cal = Calendar.getInstance();
			String loadDateStr = dateFormat.format(cal.getTime());
			
			// phase 2: load data from disk into MonetDB
			for (CopyTable table : tablesToCopy.values())
			{
				// pass load date to table
				table.setLoadDate(loadDateStr);
								
				// load new data into MonetDB
				loadData(table);
				
				// remove temp data from disk
				removeTempData(table);
			}

			LOG.info("PHASE 2 FINISHED: all data loaded into target MonetDB database");
		}
		else {
			LOG.info("Switch-Only requested with flag. Therefore, PHASE 1 and 2 skipped");
		}
					
		//if switch-ONly flag set or when the no-switch-Flag is NOT set, than switching
		if(switchOnly || !noSwitch){
			
			LOG.info("STARTING PHASE 3: switching all view-based tables to new data");
			
			// when switching only we need to find the latest load date for each table
			if (switchOnly)
			{
				for(CopyTable copyTable : tablesToCopy.values())
				{
					// find latest version of table
					String newestVersion = findNewestTable(copyTable);
					copyTable.setLoadDate(newestVersion);
				}
			}
			
			// we need another loop through the tables for temp table copying and view
			// switching. We do this after the copy actions to reduce down-time
			// phase 3: switch views (for view-based tables)
			for (CopyTable copyTable : tablesToCopy.values())
			{
				// if there are any temp table copies configured, then copy the
				// temp tables to result tables. We do this after the rest is done to
				// reduce down-time
				if (copyTable.isCopyViaTempTable())
				{
					copyTempTableToCurrentTable(copyTable);
				}
				
				// set view to current table because it contains the new data now
				if (copyTable.isUseFastViewSwitching())
				{
					if(StringUtils.isEmpty(copyTable.getLoadDate()))
					{
						LOG.error("Unable to switch view of table '" + copyTable.getToName() + "' due to missing load date");
					}
					else
					{
						MonetDBUtil.dropAndRecreateViewForTable(copyTable.getSchema(),
								copyTable.getToName(), copyTable.getCurrentTable());
					}
				}
			}
			LOG.info("PHASE 3 FINISHED: all views have been switched");
		
		
			LOG.info("STARTING PHASE 4: cleanup of data from disk and database");
			
			// phase 4: remove temp data from disk and target database
			for (CopyTable copyTable : tablesToCopy.values())
			{				
				// remove old versions of view-based tables
				// that are no longer needed
				try {
					dropOldTables(copyTable);
				} catch (SQLException e) {
					LOG.warn("Got SQLException when trying to drop older versions of table '" + copyTable.getToName() + "': " + e.getMessage(), e);
				}
			}
			
			LOG.info("PHASE 4 FINISHED: all data removed from disk and database");

		}
		else{
			LOG.info("PHASE 3 (switching) and PHASE 4 (cleanup) skipped because no-switch-flag setting");
		}
		// write out info for trigger
		if (config.isTriggerEnabled() && !anyErrors)
		{
			writeTriggerInfo(lastRunValue, lastRunColType);
		}		
		
		CopyToolConnectionManager.getInstance().closeConnections();
	
		LOG.info("Finished!");
	}
		
	/**
	 * Returns the file that stores the lastrun info
	 * @return
	 */
	public File getLastRunFile () 
	{
		LOG.info("Reading lastrun file located in directory '" + config.getTriggerDirectory() + "'");
		return new File(config.getTriggerDirectory() + "/" + config.getJobId() + "_lastrun.txt");
	}
	
	/**
	 * Checks the scheduler source (table/column) for an indication of new data
	 * Whenever the data source (e.g. ETL) has new data ready to be loaded in
	 * it should insert a new row in the scheduler source to indicate to this
	 * tool that there is a new data to be loaded.
	 */
	private boolean checkForNewData ()
	{		
		LOG.info("Checking trigger source '" + config.getTriggerSource() + "." + 
				config.getTriggerTable() + "." + config.getTriggerColumn() + "'");
		
		// get value from source
		Object newValue = null;
		int colType = -1;
		try 
		{
			Statement selectStmt =
					CopyToolConnectionManager.getInstance().getMssqlConnection(config.getTriggerSource()).createStatement();
			
			ResultSet res = selectStmt.executeQuery(
				"SELECT TOP 1 [" + config.getTriggerColumn() + "] "
				+ "FROM [" + config.getTriggerTable() + "] "
				+ "ORDER BY [" + config.getTriggerColumn() + "] DESC"
			);
			
			// no rows in table? then we cannot determine any indication
			// so we return indication of new data
			if (!res.next()) return true;			
			
			ResultSetMetaData info = res.getMetaData();
			
			colType = info.getColumnType(1);
						
			if (colType == Types.BIGINT || colType == Types.INTEGER)
			{
				colType = Types.BIGINT;
				newValue = res.getLong(1);
			}
			else if (colType == Types.DATE)
			{
				newValue = res.getDate(1);
			}
			else if (colType == Types.TIMESTAMP)
			{
				newValue = res.getTimestamp(1);
			}
			
			res.close();
			selectStmt.close();
		}
		catch (SQLException e)
		{
			LOG.warn("SQLException when trying to access scheduling source", e);
			
			// return indication of new data since we don't know for sure
			return true;
		}
		
		if (newValue == null)
		{
			// return indication of new data since we don't know for sure
			return true;
		}
		
		// load existing value from disk
		File jobFile = getLastRunFile();
		
		BufferedReader br = null;
		String oldValue = null;
		String oldColType = null;
		String oldConfigChecksum = null;
		if (jobFile.exists()) 
		{
			try {
				br = new BufferedReader(new FileReader(jobFile));
				oldValue = br.readLine();
				oldColType = br.readLine();
				oldConfigChecksum = br.readLine();
			} catch (IOException e) {
				// ignore
				LOG.warn("Unable to read existing lastrun info", e);
			} finally {
		        try {
		        	if (br != null)
		        		br.close();
				} catch (IOException e) {
					// ignore
				}
		    }
		}
		
		// set last run properties
		this.lastRunValue = newValue;
		this.lastRunColType = colType;
	        
	    if (StringUtils.isEmpty(oldValue) || StringUtils.isEmpty(oldColType) || StringUtils.isEmpty(oldConfigChecksum))
	    {
	    	// return indication of new data since we don't know for sure
			return true;
	    }
	    
	    // check if we are dealing with the same type of data
	    if (!oldColType.equals(String.valueOf(colType)))
	    {
	    	// return indication of new data since we don't know for sure
	    	return true;
	    }
	    
	    // check if we are dealing with the same config
	    if (!oldConfigChecksum.equals(config.getConfigChecksum()))
	    {
	    	// return indication of new data since we don't know for sure
	    	return true;
	    }
	    
	    LOG.info("Stored last run value: " + oldValue);
	    LOG.info("Current last run value: " + newValue);
	    
	    // check if there is newer data
	    if (colType == Types.BIGINT)
	    {
	    	Long oldNum = Long.valueOf(oldValue);
	    	Long newNum = (Long) newValue;
	    	
	    	// is new ID / long bigger than current?
	    	// then we have new data
	    	if (newNum > oldNum)
	    		return true;
	    }
	    else if (colType == Types.DATE)
	    {
	    	Date oldDate = Date.valueOf(oldValue);
	    	Date newDate = (Date) newValue;
	    	
	    	// is newer date after older date?
	    	// then we have new data
	    	if (newDate.after(oldDate))
	    		return true;
	    }
	    else if (colType == Types.TIMESTAMP)
	    {
	    	Timestamp oldTS = Timestamp.valueOf(oldValue);
	    	Timestamp newTS = (Timestamp) newValue;
	    	
	    	// is newer timestamp after older timestamp?
	    	// then we have new data
	    	if (newTS.after(oldTS))
	    		return true;
	    }
		
	    // no new data
		return false;
	}
	
	/**
	 * Writes the lastrun info for the trigger to disk
	 */
	private boolean writeTriggerInfo (Object newValue, int colType)
	{
		File scheduleFile = getLastRunFile();
		
		PrintWriter writer;
		try {
			writer = new PrintWriter(scheduleFile, "UTF-8");
			
			// write out new lastrun value
			writer.println(newValue.toString());
			
			// write out type of value
			writer.println(colType);
			
			// write out checksum of current config
			writer.println(config.getConfigChecksum());
			
			writer.close();
			
			return true;
		} catch (FileNotFoundException | UnsupportedEncodingException e) {
			LOG.warn("Unable to write scheduling info", e);
			return false;
		}
		
	}
	
	/**
	 * Find newest version of tables
	 * @throws SQLException 
	 */
	private String findNewestTable(CopyTable table) throws SQLException
	{		
		Statement q =
			CopyToolConnectionManager.getInstance().getMonetDbConnection().createStatement();
		
		String query = "SELECT name FROM sys.tables WHERE name LIKE '" + table.getToName()
				+ "_20%_%' AND name <> '" + table.getToName() + "' "
				+ "AND schema_id = (SELECT id from sys.schemas WHERE name = '" + table.getSchema()
				+ "') AND query IS NULL ORDER BY name DESC";

		LOG.trace(query);

		ResultSet result =
			q.executeQuery(query);
		
		String version = "";
		
		if (result.next())
		{
			String name = result.getString("name");
			Matcher matcher = versionPattern.matcher(name);
			
			if (matcher.find())
			{
				version = matcher.group();
			}

			LOG.info(String.format("Found schema=%s table %s with version %s", table.getSchema(), result.getString("name"), version));
			
		}
		
		result.close();
		q.close();
		
		return version;
	}
	
	/**
	 * Drops older versions of tables that are no longer used by the view.
	 * @throws SQLException 
	 */
	private void dropOldTables(CopyTable table) throws SQLException
	{
		LOG.info("Dropping older versions of table '" + table.getToName() + "'...");
		
		Statement q =
			CopyToolConnectionManager.getInstance().getMonetDbConnection().createStatement();
		
		ResultSet result =
			q.executeQuery("SELECT name FROM sys.tables WHERE name LIKE '" + table.getToName()
				+ "_20%_%' AND name <> '" + table.getToName() + "' "
				+ "AND schema_id = (SELECT id from sys.schemas WHERE name = '" + table.getSchema()
				+ "') AND query IS NULL ORDER BY name DESC");
		
		int i = 0;
		int dropCount = 0;
		Statement stmtDrop =
			CopyToolConnectionManager.getInstance().getMonetDbConnection().createStatement();
		while(result.next())
		{
			i++;
			
			// if table is a fast view-switching table then
			// 		skip first result -> is current table and referenced by view
			// 		skip second result -> as backup (TODO: perhaps make this configurable?)
			if (table.isUseFastViewSwitching())
				if (i == 1 || i == 2)
					continue;
			
			// build DROP query
			StringBuilder query = new StringBuilder("DROP TABLE ");
			
			if (!StringUtils.isEmpty(table.getSchema()))
				query.append(MonetDBUtil.quoteMonetDbIdentifier(table.getSchema())).append(".");
			
			query.append(MonetDBUtil.quoteMonetDbIdentifier(result.getString("name"))).append(";");
			
			// execute DROP query
			stmtDrop.executeUpdate(query.toString());			
			dropCount++;
		}
		
		if (i == 0 || (table.isUseFastViewSwitching() && i <= 2))
			LOG.info("Table '" + table.getToName() + "' has no older versions");
		else
			LOG.info("Dropped " + dropCount + " older versions of table '" + table.getToName() + "'");
		
		result.close();
		q.close();
	}
	
	/**
	 * Removes temp data from local disk
	 * 
	 */
	private void removeTempData(CopyTable table)
	{
		File dataFile = new File(config.getTempDirectory(), table.getTempFilePrefix() + "_data.csv");
		File countFile = new File(config.getTempDirectory(), table.getTempFilePrefix() + "_count.txt");
		File metaDataFile = new File(config.getTempDirectory(), table.getTempFilePrefix() + "_metadata.ser");
		
		dataFile.delete();
		countFile.delete();
		metaDataFile.delete();
	}
	
	/**
	 * Copies data from a MSSQL table to local disk, including meta data and row count.
	 * @throws SQLException 
	 */
	private void copyData(CopyTable table) throws Exception
	{
		LOG.info("Starting with copy of data from table " + table.getFromName() + " to disk...");
		
		// select data from MS SQL Server
		Statement selectStmt =
			CopyToolConnectionManager.getInstance().getMssqlConnection(table.getSource()).createStatement();

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
		
		String tmpDir = config.getTempDirectory();
		
		String tmpFilePrefix = table.getTempFilePrefix();
		
		// serialize meta data to disk
		File metaDataFile = new File(tmpDir, tmpFilePrefix + "_metadata.ser");
		FileOutputStream fileOut = new FileOutputStream(metaDataFile);
		ObjectOutputStream out = new ObjectOutputStream(fileOut);
		out.writeObject(new SerializableResultSetMetaData(metaData));
		out.close();
		fileOut.close();
		LOG.info("Serialized metadata to temp file: " + metaDataFile.getAbsolutePath());
		
		// write data to disk
		File temp = new File(tmpDir, tmpFilePrefix + "_data.csv");		
		LOG.info("Writing data to temp file: " + temp.getAbsolutePath());
		
		BufferedWriter bw = new BufferedWriter
			    (new OutputStreamWriter(new FileOutputStream(temp), "UTF-8"));

		long startTime = System.currentTimeMillis();
		long insertCount = 0;
		int columnCount = metaData.getColumnCount();
		
		while (resultSet.next())
		{
			for (int i = 1; i <= columnCount; i++)
			{
				Object value = resultSet.getObject(i);

				if (value == null)
				{
					bw.write("\\N");
				}
				else
				{
					String valueStr = value.toString();

					// escape \ with \\
					valueStr = valueStr.replaceAll("\\\\", "\\\\\\\\");

					// escape " with \"
					valueStr = valueStr.replaceAll("\"", "\\\\\"");
					
					bw.write("\"" + valueStr + "\"");
				}			

				// column separator (not for last column)
				if (i < columnCount)
				{
					bw.write(",");
				}
			}

			// record separator
			bw.newLine();

			insertCount++;

			if (insertCount % 100000 == 0)
			{
				bw.flush();
				printInsertProgress(startTime, insertCount, rowCount, "written to disk");
			}
		}
		bw.flush();
		bw.close();
		printInsertProgress(startTime, insertCount, rowCount, "written to disk");
		
		// write insert count to disk as well
		File countFile = new File(tmpDir, tmpFilePrefix + "_count.txt");
		Writer wr = new FileWriter(countFile);
		wr.write(String.valueOf(insertCount));
		wr.close();
		LOG.info("Written row count to temp file: " + countFile.getAbsolutePath());
		
		LOG.info("Finished copying data of table " + table.getFromName() + " to disk!");
	}
	
	/**
	 * Loads data from disk into MonetDB, as efficiently as possible. Tries to do so
	 * using various methods.
	 */
	private void loadData(CopyTable table) throws Exception
	{
		LOG.info("Starting to load data of table " + table.getFromName() + " into MonetDB...");
		long startTime = System.currentTimeMillis();
		
		// verify all temp files are available
		File dataFile = new File(config.getTempDirectory(), table.getTempFilePrefix() + "_data.csv");
		File countFile = new File(config.getTempDirectory(), table.getTempFilePrefix() + "_count.txt");
		File metaDataFile = new File(config.getTempDirectory(), table.getTempFilePrefix() + "_metadata.ser");
		
		if (!dataFile.exists())
		{
			throw new Exception("Missing temporary data file for table '" + table.getFromName() + "'");
		}
		
		if (!countFile.exists())
		{
			throw new Exception("Missing temporary count file for table '" + table.getFromName() + "'");
		}
		
		if (!metaDataFile.exists())
		{
			throw new Exception("Missing temporary metadata file for table '" + table.getFromName() + "'");
		}
		
		// read count
		BufferedReader br = new BufferedReader(new FileReader(countFile));
		String countStr = br.readLine();
		br.close();
		
		Long insertCount = null;
		try {
			insertCount = Long.parseLong(countStr);
		} catch (NumberFormatException e) {
			throw new Exception("Unable to read row count from temporary count file for table '" + table.getFromName() + "'");
		}
		
		if (insertCount == null)
			throw new Exception("Unable to read row count from temporary count file for table '" + table.getFromName() + "'");
		
		// read metadata
		SerializableResultSetMetaData metaData = null;
		try {
			FileInputStream fileIn = new FileInputStream(metaDataFile);
		    ObjectInputStream in = new ObjectInputStream(fileIn);
		    metaData = (SerializableResultSetMetaData) in.readObject();
		    in.close();
		    fileIn.close();
		} catch (IOException | ClassNotFoundException e) {
			throw new Exception("Unable to read metadata from temporary metadata file for table '" + table.getFromName() + "'", e);
		}
		
		if (metaData == null)
			throw new Exception("Unable to read metadata from temporary metadata file for table '" + table.getFromName() + "'");
		
		MonetDBTable copyToTable =
			table.isCopyViaTempTable() ? table.getTempTable() : table.getCurrentTable();

		// check tables in monetdb
		checkTableInMonetDb(copyToTable, metaData);
		
		// do truncate?
		if (table.truncate())
		{
			MonetDBUtil.truncateMonetDBTable(copyToTable);
		}
		
		// load data
		boolean isLoaded = false;
		
		// is it allowed to use COPY INTO method?
		if (copyToTable.getCopyTable().getCopyMethod() != CopyTable.COPY_METHOD_INSERT)
		{
			// try to load directly via COPY INTO FROM FILE
			try {
				isLoaded = loadDataFromFile(copyToTable, dataFile, metaData, insertCount, table.isUseLockedMode());
			} catch (SQLException e) {
				LOG.warn("Failed to load data directly from file: " + e.getMessage());
			}
					
			// not loaded? then try loading via COPY INTO FROM STDIN
			if (!isLoaded)
			{
				try {
					isLoaded = loadDataFromStdin (copyToTable, dataFile, metaData, insertCount, table.isUseLockedMode());
				} catch (Exception e) {
					LOG.warn("Failed to load data directly via STDIN: " + e.getMessage());
				}
			}
		}
		
		// still not loaded? final try with manual INSERTs
		if (!isLoaded)
		{
			try {
				isLoaded = loadDataWithInserts (copyToTable, dataFile, metaData, insertCount);
			} catch (Exception e) {
				LOG.error("Failed to load data with INSERTs: " + e.getMessage());
			}
		}
		
		// still not loaded? then unable to load, throw exception
		if (!isLoaded) 
		{
			throw new Exception("Unable to load data into MonetDB for table " + table.getFromName());
		}
		
		long loadTime = (System.currentTimeMillis() - startTime) / 1000;
		LOG.info("Finished loading data into " + copyToTable.getName() + " in " + loadTime + " seconds");
	}
	
	private boolean loadDataWithInserts(MonetDBTable monetDBTable, File dataFile,
			ResultSetMetaData metaData, long rowCount) throws SQLException, IOException
	{
		LOG.info("Loading data with INSERTs into table " + monetDBTable.getToTableSql() + "...");
		LOG.info("Batch size set: " + config.getBatchSize());

		// build insert SQL
		StringBuilder insertSql = new StringBuilder("INSERT INTO ");
		insertSql.append(monetDBTable.getToTableSql());
		insertSql.append(" (");

		String[] colNames = new String[metaData.getColumnCount()];
		String[] values = new String[metaData.getColumnCount()];

		for (int i = 1; i <= metaData.getColumnCount(); i++)
		{
			String colName = metaData.getColumnName(i).toLowerCase();
			colNames[i - 1] = MonetDBUtil.quoteMonetDbIdentifier(colName);
		}

		insertSql.append(StringUtils.join(colNames, ","));
		insertSql.append(")");
		insertSql.append(" VALUES (");

		Statement insertStmt =
			CopyToolConnectionManager.getInstance().getMonetDbConnection().createStatement();

		CopyToolConnectionManager.getInstance().getMonetDbConnection().setAutoCommit(false);

		long startTime = System.currentTimeMillis();

		int batchCount = 0;
		long insertCount = 0;
		
	    CSVReader reader = new CSVReader(new FileReader(dataFile));
	    String [] line;
	    while ((line = reader.readNext()) != null)
		{
			for (int i = 1; i <= metaData.getColumnCount(); i++)
			{
				String value = line[i-1];

				if (value.equals("\\N"))
				{
					values[i - 1] = "NULL";
				}
				else
				{
					values[i - 1] = MonetDBUtil.quoteMonetDbValue(value);
				}
			}

			StringBuilder insertRecordSql = new StringBuilder(insertSql);
			insertRecordSql.append(StringUtils.join(values, ","));
			insertRecordSql.append(")");

			insertStmt.addBatch(insertRecordSql.toString());
			batchCount++;

			if (batchCount % config.getBatchSize() == 0)
			{
				LOG.info("Inserting next batch of " + config.getBatchSize() + " records...");

				insertStmt.executeBatch();
				CopyToolConnectionManager.getInstance().getMonetDbConnection().commit();

				insertStmt.clearBatch();
				insertCount = insertCount + batchCount;
				batchCount = 0;

				printInsertProgress(startTime, insertCount, rowCount);
			}
		}
		
		reader.close();

		if (batchCount > 0)
		{
			LOG.info("Inserting final batch of " + batchCount + " records...");

			insertStmt.executeBatch();
			CopyToolConnectionManager.getInstance().getMonetDbConnection().commit();

			insertStmt.clearBatch();
			insertCount = insertCount + batchCount;

			printInsertProgress(startTime, insertCount, rowCount);
		}

		CopyToolConnectionManager.getInstance().getMonetDbConnection().setAutoCommit(true);

		return true;
	}
	
	private boolean loadDataFromStdin (MonetDBTable monetDBTable, File dataFile,
		ResultSetMetaData metaData, long rowCount, boolean useLockedMode)  
		throws Exception
	{
		LOG.info("Loading data via STDIN into " + monetDBTable.getToTableSql());
		
		BufferedMCLReader in =
			CopyToolConnectionManager.getInstance().getMonetDbServer().getReader();
		BufferedMCLWriter out =
			CopyToolConnectionManager.getInstance().getMonetDbServer().getWriter();

		String error = in.waitForPrompt();
		if (error != null)
			throw new Exception(error);
		
		StringBuilder query = new StringBuilder();
		query.append("COPY ");
		
		if (rowCount > 0)
			query.append(rowCount).append(" RECORDS ");
		
		query.append(" INTO ").append(monetDBTable.getToTableSql());		
		query.append(" FROM STDIN USING DELIMITERS ',','\\n','\"' NULL AS '\\\\N'");
		
		if (useLockedMode)
			query.append(" LOCKED");
		
		query.append(";");

		// the leading 's' is essential, since it is a protocol
		// marker that should not be omitted, likewise the
		// trailing semicolon
		out.write('s');
		out.write(query.toString());
		out.newLine();

		long startTime = System.currentTimeMillis();
		long insertCount = 0;
		
		BufferedReader br = new BufferedReader(
			new InputStreamReader(
				new FileInputStream(dataFile), 
				"UTF8"
			)
		);

		String line;
		while((line = br.readLine()) != null)
		{
			// write out record
			out.write(line);

			// record separator
			out.newLine();

			insertCount++;

			if (insertCount % 100000 == 0)
			{
				printInsertProgress(startTime, insertCount, rowCount, "processed");
			}
		}
		printInsertProgress(startTime, insertCount, rowCount, "processed");
		br.close();
		
		LOG.info("Finalising COPY INTO... this may take a while!");

		out.writeLine("");

		error = in.waitForPrompt();
		if (error != null)
			throw new Exception(error);
		
		return true;
	}
	
	private boolean loadDataFromFile (MonetDBTable monetDBTable, File dataFile,
			ResultSetMetaData metaData, long rowCount, boolean useLockedMode) throws SQLException
	{
		LOG.info("Loading data directly from file into " + monetDBTable.getToTableSql());
		
		Statement copyStmt =
				CopyToolConnectionManager.getInstance().getMonetDbConnection().createStatement();
				
		StringBuilder query = new StringBuilder();
		query.append("COPY ");
		
		if (rowCount > 0)
			query.append(rowCount).append(" RECORDS ");
		
		query.append(" INTO ").append(monetDBTable.getToTableSql());
		query.append(" FROM '").append(dataFile.getAbsolutePath()).append("'");
		query.append(" USING DELIMITERS ',','\\n','\"' NULL AS '\\\\N'");
		
		if (useLockedMode)
			query.append(" LOCKED");
		
		query.append(";");
		
		// execute COPY INTO statement
		copyStmt.execute(query.toString());
		
		copyStmt.close();
		
		return true;
	}

	private void checkTableInMonetDb(MonetDBTable monetDBTable, ResultSetMetaData metaData)
			throws SQLException
	{
		boolean tableExists = MonetDBUtil.monetDBTableExists(monetDBTable);
		
		// can't auto create?
		if (tableExists == false && monetDBTable.getCopyTable().create() == false)
		{
			throw new SQLException("Table " + monetDBTable.getToTableSql()
				+ " does not exist in MonetDB database and auto-create is set to false");
		}

		// need to drop? don't drop when useFastViewSwitching is enabled because then we
		// have a view
		if (!monetDBTable.getCopyTable().isUseFastViewSwitching())
		{
			if (tableExists && monetDBTable.getCopyTable().drop())
			{
				MonetDBUtil.dropMonetDBTableOrView(monetDBTable);
				tableExists = false;
			}
		}

		if (tableExists)
		{
			// verify if table is as expected
			MonetDBUtil.verifyColumnsOfExistingTable(monetDBTable, metaData);
		}
		else
		{
			MonetDBUtil.createMonetDBTable(monetDBTable, metaData);
		}
	}

	private void copyTempTableToCurrentTable(CopyTable copyTable) throws SQLException
	{
		// create table schema.temptable as select * from schema.currentTable with data;
		LOG.info("Copying the temp table to the result table");

		// drop result table before replacing with temp table
		if (MonetDBUtil.monetDBTableExists(copyTable.getCurrentTable()))
		{
			MonetDBUtil.dropMonetDBTable(copyTable.getCurrentTable());
		}
		MonetDBUtil.copyMonetDBTableToNewMonetDBTable(copyTable.getTempTable(),
			copyTable.getCurrentTable());
		// drop temp table, we wont need it anymore
		MonetDBUtil.dropMonetDBTable(copyTable.getTempTable());


		LOG.info("Finished copying the temp table to the result table");
	}

	

	private void printInsertProgress (long startTime, long insertCount, long rowCount)
	{
		printInsertProgress(startTime, insertCount, rowCount, "inserted");
	}
	
	private void printInsertProgress(long startTime, long insertCount, long rowCount, String action)
	{
		long totalTime = System.currentTimeMillis() - startTime;

		// how much time for current inserted records?
		float timePerRecord = (float) (totalTime / 1000) / (float) insertCount;

		long timeLeft = Float.valueOf((rowCount - insertCount) * timePerRecord).longValue();

		LOG.info("Records " + action);
		float perc = ((float) insertCount / (float) rowCount) * 100;
		LOG.info("Progress: " + insertCount + " out of " + rowCount + " ("
			+ formatPerc.format(perc) + "%)");
		LOG.info("Time: " + (totalTime / 1000) + " seconds spent; estimated time left is "
			+ timeLeft + " seconds");
	}
	
	private void loadDatabaseDrivers () throws ClassNotFoundException
	{
		// make sure JDBC drivers are loaded
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		Class.forName("net.sourceforge.jtds.jdbc.Driver");
	}

}
