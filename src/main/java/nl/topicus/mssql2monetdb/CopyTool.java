package nl.topicus.mssql2monetdb;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DecimalFormat;
import java.util.HashMap;

import nl.cwi.monetdb.mcl.io.BufferedMCLReader;
import nl.cwi.monetdb.mcl.io.BufferedMCLWriter;
import nl.topicus.mssql2monetdb.util.EmailUtil;
import nl.topicus.mssql2monetdb.util.MonetDBUtil;
import nl.topicus.mssql2monetdb.util.MssqlUtil;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public class CopyTool
{
	private static final Logger LOG = Logger.getLogger(CopyTool.class);

	private CopyToolConfig config;

	private DecimalFormat formatPerc = new DecimalFormat("#.#");
	
	private Object lastRunValue;
	
	private int lastRunColType;

	/**
	 * @param args
	 */
	public static void main(String[] args)
	{
		//the log files is not yet inistialized
		System.out.println("Started MSSQL2MonetDB copy tool");

		// run tool
		(new CopyTool(new CopyToolConfig(args))).run();
	}
	
	

	public CopyTool(CopyToolConfig config)
	{
		this.config = config;
		if (config == null)
		{
			LOG.error("CopyToolConfig cannot be null");
			System.exit(1);
		}
	}

	
	
	public void run()
	{
		// load database drivers
		loadDatabaseDrivers();
		
		// check if another instance of this job is already running
		if (isInstanceRunning() && !config.allowMultipleInstances())
		{
			LOG.info("Another instance of this job is already running at the moment and multiple instances have not been enabled");
			LOG.info("Finished!");
			return;
		}
		
		// mark as running
		try {
			markAsRunning();
		} catch (IOException e) {
			LOG.error("Unable to mark job as running", e);
			EmailUtil.sendMail("Unable to start Copy job with the following error: "+ e.getStackTrace(), "Unable to copy data from table in monetdb", config.getDatabaseProperties());
		}
		
		HashMap<String, CopyTable> tablesToCopy = config.getTablesToCopy();
		if (tablesToCopy.size() > 0)
		{
			CopyToolConnectionManager.getInstance().openConnections(config);
			
			// check if scheduling is enabled and if so, if there is any new data
			boolean anyErrors = false;
			if (config.isSchedulingEnabled() && !checkForNewData())
			{
				LOG.info("No indication of new data from scheduling source '" + config.getSchedulerTable() + "." + config.getSchedulerColumn() + "'");
			}
			else
			{
				// check if all MSSQL tables have data and stop the copy if one doesn't
				if (MssqlUtil.allMSSQLTablesHaveData(tablesToCopy))
				{
					for (CopyTable table : tablesToCopy.values())
					{
						try
						{
							// backup table if configured and switch view to the backup table
							if (table.isUseFastViewSwitching())
								backupTableAndSwitchView(table.getCurrentTable());
	
							// copy new data to monetdb
							copyTable(table);
						}
						catch (SQLException e)
						{
							anyErrors = true;
							LOG.error("Unable to copy data from table '" + table.getFromName() + "'", e);
							EmailUtil.sendMail("Unable to copy data from table" + table.getFromName() + " with the following error: "+ e.getStackTrace(), "Unable to copy data from table in monetdb", config.getDatabaseProperties());
						}
					}
	
					// we need another loop through the tables for temp table copying and view
					// switching. We do this after the copy actions to reduce down-time
					for (CopyTable copyTable : tablesToCopy.values())
					{
						// if there are any temp table copies configured, then copy the
						// temp tables to result tables. We do this after the rest is done to
						// reduce down-time
						if (copyTable.isCopyViaTempTable())
						{
							copyTempTableToCurrentTable(copyTable);
						}
						try
						{
							// set view to current table because it contains the new data now
							if (copyTable.isUseFastViewSwitching())
								MonetDBUtil.dropAndRecreateViewForTable(copyTable.getSchema(),
									copyTable.getToName(), copyTable.getCurrentTable());
						}
						catch (SQLException e)
						{
							anyErrors = true;
							LOG.error("Unable to create view '" + copyTable.getToViewSql() + "'", e);
							EmailUtil.sendMail("Unable to create view" + copyTable.getToViewSql() + " with the following error: "+ e.toString(), "Unable to create view in monetdb", config.getDatabaseProperties());
						}
					}
				}
			}
			
			// write out info for schedule
			if (config.isSchedulingEnabled() && !anyErrors)
			{
				writeScheduleInfo(lastRunValue, lastRunColType);
			}
		}

		CopyToolConnectionManager.getInstance().closeConnections();
		
		markAsFinished();

		LOG.info("Finished!");
	}
	
	/**
	 * Checks whether another instance of the same job is already running on the same server
	 */
	public boolean isInstanceRunning ()
	{
		File runFile = getRunFile();
		return runFile.exists();
	}
	
	/**
	 * Marks the job as running
	 * @throws IOException 
	 */
	public void markAsRunning () throws IOException
	{
		File runFile = getRunFile();
		runFile.createNewFile();
		LOG.info("Marked job as currently running");
	}
	
	/**
	 * Mark the job as finished (no longer running)
	 */
	public void markAsFinished ()
	{
		File runFile = getRunFile();
		if (runFile.exists())
		{
			runFile.delete();
			LOG.info("Marked job as finished");
		}
	}
	
	public File getRunFile ()
	{
		return new File(getUserDir().getAbsolutePath() + "/." + config.getJobId() + "-running");
	}
	
	public File getUserDir ()
	{
		return new File(System.getProperty("user.dir"));
	}
	
	/**
	 * Returns the file that stores the lastrun info
	 * @return
	 */
	public File getLastRunFile () 
	{
		return new File(getUserDir().getAbsolutePath() + "/" + config.getJobId() + "_lastrun.txt");
	}
	
	/**
	 * Checks the scheduler source (table/column) for an indication of new data
	 * Whenever the data source (e.g. ETL) has new data ready to be loaded in
	 * it should insert a new row in the scheduler source to indicate to this
	 * tool that there is a new data to be loaded.
	 */
	private boolean checkForNewData ()
	{		
		LOG.info("Checking scheduling source '" + config.getSchedulerSource() + "." + 
				config.getSchedulerTable() + "." + config.getSchedulerColumn() + "'");
		
		// get value from source
		Object newValue = null;
		int colType = -1;
		try 
		{
			Statement selectStmt =
					CopyToolConnectionManager.getInstance().getMssqlConnection(config.getSchedulerSource()).createStatement();
			
			ResultSet res = selectStmt.executeQuery(
				"SELECT TOP 1 [" + config.getSchedulerColumn() + "] "
				+ "FROM [" + config.getSchedulerTable() + "] "
				+ "ORDER BY [" + config.getSchedulerColumn() + "] DESC"
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
	 * Writes the lastrun info for the scheduling to disk
	 */
	private boolean writeScheduleInfo (Object newValue, int colType)
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
	 * Backup a table by copying the data into a backup table named backup_currentTable by
	 * currentTable and metaData of the MSSQL table that is copied.
	 * 
	 * @throws SQLException
	 */
	private void backupTableAndSwitchView(MonetDBTable currentTable) throws SQLException
	{
		LOG.info("Backupping " + currentTable.getToTableSql());
		
		// create a MonetDBTable object for the backup table
		MonetDBTable backupTable = new MonetDBTable(currentTable.getCopyTable());
		
		// use the copyTable.toName
		backupTable.setName(currentTable.getCopyTable().getToName());
		backupTable.setBackupTable(true);
		
		// drop and recreate backup table regardless if it exists
		LOG.info("Drop backup table '" + backupTable.getToTableSql() + "' if exists");
		MonetDBUtil.dropMonetDBTable(backupTable);
		
		// copy from currentTable to backup table
		MonetDBUtil.copyMonetDBTableToNewMonetDBTable(currentTable, backupTable);
		
		LOG.info("Switch view to '" + backupTable.getToTableSql() + "'");
		
		// drop and recreate the view for the table and point to the just created backup
		// table so we can fill the current table with new data
		MonetDBUtil.dropAndRecreateViewForTable(currentTable.getCopyTable().getSchema(),
			currentTable.getCopyTable().getToName(), backupTable);
	}

	/**
	 * Copy a MSSQL table to MonetDB. This will copy the MSSQL data to the result table or
	 * a temporary table if configured that way. This includes auto-creating the necessary
	 * tables and importing the data using selects or copy into.
	 */
	private void copyTable(CopyTable table) throws SQLException
	{
		LOG.info("Starting with copy of table " + table.getFromName() + "...");

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

		MonetDBTable copyToTable =
			table.isCopyViaTempTable() ? table.getTempTable() : table.getCurrentTable();

		// check tables in monetdb
		checkTableInMonetDb(copyToTable, metaData);

		// do truncate?
		if (table.truncate())
		{
			MonetDBUtil.truncateMonetDBTable(copyToTable);
		}

		// copy data
		if (table.getCopyMethod() == CopyTable.COPY_METHOD_COPYINTO
			&& CopyToolConnectionManager.getInstance().getMonetDbServer() != null)
		{
			try
			{
				copyDataWithCopyInto(copyToTable, resultSet, metaData, rowCount, table.isUseLockedMode());
			}
			catch (Exception e)
			{
				LOG.error("Copying data failed", e);
				EmailUtil.sendMail("Copying data failed with the following error: "+ e.getStackTrace(), "Copying failed in monetdb", config.getDatabaseProperties());
			}
		}
		else if (table.getCopyMethod() == CopyTable.COPY_METHOD_COPYINTO_VIA_TEMP_FILE)
		{
			try {
				copyDataWithCopyIntoViaTempFile(copyToTable, resultSet, metaData, rowCount, table.isUseLockedMode());
			}
			catch (Exception e)
			{
				LOG.error("Copying data failed", e);
				EmailUtil.sendMail("Copying data failed with the following error: "+ e.getStackTrace(), "Copying failed in monetdb", config.getDatabaseProperties());
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
				EmailUtil.sendMail("Copying data failed with the following error: "+ e.getStackTrace(), "Copying failed in monetdb", config.getDatabaseProperties());

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
		if (tableExists && monetDBTable.getCopyTable().drop())
		{
			MonetDBUtil.dropMonetDBTable(monetDBTable);
			tableExists = false;
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

	private void copyTempTableToCurrentTable(CopyTable copyTable)
	{
		// create table schema.temptable as select * from schema.currentTable with data;
		LOG.info("Copying the temp table to the result table");
		try
		{
			// drop result table before replacing with temp table
			if (MonetDBUtil.monetDBTableExists(copyTable.getCurrentTable()))
			{
				MonetDBUtil.dropMonetDBTable(copyTable.getCurrentTable());
			}
			MonetDBUtil.copyMonetDBTableToNewMonetDBTable(copyTable.getTempTable(),
				copyTable.getCurrentTable());
			// drop temp table, we wont need it anymore
			MonetDBUtil.dropMonetDBTable(copyTable.getTempTable());
		}
		catch (SQLException e)
		{
			LOG.error("Error copying temp table to current table", e);

			EmailUtil.sendMail("Error copying temp table to current table: "+ e.getStackTrace(), "Error copying temp table to current table in monetdb", config.getDatabaseProperties());
		}

		LOG.info("Finished copying the temp table to the result table");
	}
	
	private void copyDataWithCopyIntoViaTempFile (MonetDBTable monetDBTable, ResultSet resultSet,
			ResultSetMetaData metaData, long rowCount, boolean useLockedMode) throws Exception
	{
		LOG.info("Using COPY INTO VIA TEMP FILE to copy data to table " + monetDBTable.getToTableSql() + "...");
	
		File temp = File.createTempFile("table_" + monetDBTable.getName(), ".csv");		
		LOG.info("Writing data to temp file: " + temp.getAbsolutePath());
		
		BufferedWriter bw = new BufferedWriter(new FileWriter(temp));

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

				bw.write("\"" + valueStr + "\"");

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
				printInsertProgress(startTime, insertCount, rowCount);
			}
		}
		bw.flush();
		bw.close();
		printInsertProgress(startTime, insertCount, rowCount);

		LOG.info("Finalising COPY INTO... this may take a while!");
		
		Statement copyStmt =
				CopyToolConnectionManager.getInstance().getMonetDbConnection().createStatement();
		
		String query =
			"COPY " + insertCount + " RECORDS INTO " + monetDBTable.getToTableSql()
				+ " FROM '" + temp.getAbsolutePath() + "' USING DELIMITERS ',','\\n','\"' NULL AS ''";
				
		if (useLockedMode)
		{
			query += " LOCKED";
		}
		
		query += ";";
		
		// execute COPY INTO statement
		copyStmt.execute(query);
		
		copyStmt.close();
		
		try {
			temp.delete();
		} catch (SecurityException e) {
			// don't care
		}	
		
		LOG.info("Finished copying data");	
	}

	private void copyDataWithCopyInto(MonetDBTable monetDBTable, ResultSet resultSet,
			ResultSetMetaData metaData, long rowCount, boolean useLockedMode) throws Exception
	{
		LOG.info("Using COPY INTO to copy data to table " + monetDBTable.getToTableSql() + "...");

		BufferedMCLReader in =
			CopyToolConnectionManager.getInstance().getMonetDbServer().getReader();
		BufferedMCLWriter out =
			CopyToolConnectionManager.getInstance().getMonetDbServer().getWriter();

		String error = in.waitForPrompt();
		if (error != null)
			throw new Exception(error);

		String query =
			"COPY INTO " + monetDBTable.getToTableSql()
				+ " FROM STDIN USING DELIMITERS ',','\\n','\"' NULL AS ''";
				
		if (useLockedMode)
		{
			query += " LOCKED";
		}
		
		query += ";";

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

	private void copyData(MonetDBTable monetDBTable, ResultSet resultSet,
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
				else if (value instanceof String || value instanceof Timestamp || value instanceof Clob)
				{
					values[i - 1] = MonetDBUtil.quoteMonetDbValue(value.toString());
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

		LOG.info("Finished copying data");
	}

	private void printInsertProgress(long startTime, long insertCount, long rowCount)
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
	
	private void loadDatabaseDrivers ()
	{
		// make sure JDBC drivers are loaded
		try
		{
			Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		}
		catch (ClassNotFoundException e)
		{
			LOG.fatal("Unable to load MonetDB JDBC driver");
			EmailUtil.sendMail("Unable to load MonetDB JDBC driverwith the following error: "+ e.getStackTrace(), "Unable to load MonetDB JDBC driver in monetdb", config.getDatabaseProperties());
			System.exit(1);
		}

		try
		{
			Class.forName("net.sourceforge.jtds.jdbc.Driver");
		}
		catch (ClassNotFoundException e)
		{
			LOG.fatal("Unable to load MS SQL jTDS JDBC driver");
			EmailUtil.sendMail("Unable to load MS SQL jTDS JDBC driver with the following error: "+ e.getStackTrace(), "Unable to load MS SQL jTDS JDBC driver in monetdb", config.getDatabaseProperties());
			System.exit(1);
		}
	}

}
