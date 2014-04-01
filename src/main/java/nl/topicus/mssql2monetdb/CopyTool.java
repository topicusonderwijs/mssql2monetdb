package nl.topicus.mssql2monetdb;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
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

	/**
	 * @param args
	 */
	public static void main(String[] args)
	{
		LOG.info("Started MSSQL2MonetDB copy tool");

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
		HashMap<String, CopyTable> tablesToCopy = config.getTablesToCopy();
		if (tablesToCopy.size() > 0)
		{
			CopyToolConnectionManager.getInstance().openConnections(config.getDatabaseProperties());

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
						LOG.error("Unable to copy data from table '" + table.getFromName() + "'", e);
						EmailUtil.sendMail("Unable to copy data from table" + table.getFromName() + "met de volgende foutmelding: "+ e.toString(), "Unable to copy data from table in monetdb", config.getDatabaseProperties());
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
						LOG.error("Unable to create view '" + copyTable.getToViewSql() + "'", e);
						EmailUtil.sendMail("Unable to create view" + copyTable.getToViewSql() + "met de volgende foutmelding: "+ e.toString(), "Unable to create view in monetdb", config.getDatabaseProperties());
					}
				}
			}
		}

		CopyToolConnectionManager.getInstance().closeConnections();

		LOG.info("Finished!");
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
			CopyToolConnectionManager.getInstance().getMssqlConnection().createStatement();

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
				copyDataWithCopyInto(copyToTable, resultSet, metaData, rowCount);
			}
			catch (Exception e)
			{
				LOG.error("Copying data failed", e);

				EmailUtil.sendMail("Copying data failed met de volgende foutmelding: "+ e.getStackTrace(), "Copying failed in monetdb", config.getDatabaseProperties());
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
				
				EmailUtil.sendMail("Copying data failed met de volgende foutmelding: "+ e.getStackTrace(), "Copying failed in monetdb", config.getDatabaseProperties());

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

			EmailUtil.sendMail("Error copying temp table to current table met de volgende foutmelding: "+ e.getStackTrace(), "Error copying temp table to current table in monetdb", config.getDatabaseProperties());
		}

		LOG.info("Finished copying the temp table to the result table");
	}

	private void copyDataWithCopyInto(MonetDBTable monetDBTable, ResultSet resultSet,
			ResultSetMetaData metaData, long rowCount) throws Exception
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
				else if (value instanceof String || value instanceof Timestamp)
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

}
