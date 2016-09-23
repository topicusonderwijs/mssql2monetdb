package nl.topicus.mssql2monetdb.load;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVReader;
import nl.cwi.monetdb.mcl.io.BufferedMCLReader;
import nl.cwi.monetdb.mcl.io.BufferedMCLWriter;
import nl.topicus.mssql2monetdb.CopyTable;
import nl.topicus.mssql2monetdb.CopyTool;
import nl.topicus.mssql2monetdb.CopyToolConnectionManager;
import nl.topicus.mssql2monetdb.MonetDBTable;
import nl.topicus.mssql2monetdb.util.MonetDBUtil;
import nl.topicus.mssql2monetdb.util.SerializableResultSetMetaData;

public class LoadDataFromCsv extends AbstractLoadData 
{
	private static final Logger LOG =  LoggerFactory.getLogger(LoadDataFromCsv.class);

	public void loadData (SerializableResultSetMetaData metaData, CopyTable table, MonetDBTable copyToTable, Long insertCount) throws Exception
	{
		File dataFile = new File(getConfig().getTempDirectory(), table.getTempFilePrefix() + "_data.csv");
		
		// verify data file exists
		if (!dataFile.exists())
		{
			throw new Exception("Missing temporary data file for '" + table.getDescription() + "'");
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
			throw new Exception("Unable to load data into MonetDB for '" + table.getDescription() + "'");
		}
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
				CopyTool.printInsertProgress(startTime, insertCount, rowCount, "processed");
			}
		}
		CopyTool.printInsertProgress(startTime, insertCount, rowCount, "processed");
		br.close();
		
		LOG.info("Finalising COPY INTO... this may take a while!");

		out.writeLine("");

		error = in.waitForPrompt();
		if (error != null)
			throw new Exception(error);
		
		return true;
	}
	
	private boolean loadDataWithInserts(MonetDBTable monetDBTable, File dataFile,
			ResultSetMetaData metaData, long rowCount) throws SQLException, IOException
	{
		LOG.info("Loading data with INSERTs into table " + monetDBTable.getToTableSql() + "...");
		LOG.info("Batch size set: " + getConfig().getBatchSize());

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

			if (batchCount % getConfig().getBatchSize() == 0)
			{
				LOG.info("Inserting next batch of " + getConfig().getBatchSize() + " records...");

				insertStmt.executeBatch();
				CopyToolConnectionManager.getInstance().getMonetDbConnection().commit();

				insertStmt.clearBatch();
				insertCount = insertCount + batchCount;
				batchCount = 0;

				CopyTool.printInsertProgress(startTime, insertCount, rowCount);
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

			CopyTool.printInsertProgress(startTime, insertCount, rowCount);
		}

		CopyToolConnectionManager.getInstance().getMonetDbConnection().setAutoCommit(true);

		return true;
	}
}
