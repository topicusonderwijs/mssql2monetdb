package nl.topicus.mssql2monetdb.load;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Statement;
import java.util.StringJoiner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nl.topicus.mssql2monetdb.CopyTable;
import nl.topicus.mssql2monetdb.CopyToolConnectionManager;
import nl.topicus.mssql2monetdb.MonetDBTable;
import nl.topicus.mssql2monetdb.copy.binary.ValueConverter;
import nl.topicus.mssql2monetdb.util.MonetDBUtil;
import nl.topicus.mssql2monetdb.util.SerializableResultSetMetaData;

public class LoadDataFromBinary extends AbstractLoadData 
{
	private static final Logger LOG =  LoggerFactory.getLogger(LoadDataFromBinary.class);
	
	@Override
	public void loadData(SerializableResultSetMetaData metaData, CopyTable table, MonetDBTable copyToTable,
			Long insertCount) throws Exception 
	{
		LOG.info("Loading data directly from binary files into " + copyToTable.getToTableSql());
		
		String tmpDir = getConfig().getTempDirectory();
		String tmpFilePrefix = table.getTempFilePrefix();
		
		int columnCount = metaData.getColumnCount();
		
		StringJoiner dataFiles = new StringJoiner("','");
		for (int i = 1; i <= columnCount; i++)
		{
			File file = new File(tmpDir, tmpFilePrefix + "_data_" + i + ".bin");

			if (!file.exists())
			{
				throw new Exception(String.format("Missing data file for table '%s' for column '%s'", copyToTable.getName(), i));
			}
			
			dataFiles.add(file.getAbsolutePath());
		}
		
		StringBuilder query = new StringBuilder();
		query.append("COPY BINARY INTO ");
		query.append(copyToTable.getToTableSql());
		query.append(" FROM ('");
		query.append(dataFiles.toString());		
		query.append("');");
		
		try (Statement copyStmt =
				CopyToolConnectionManager.getInstance().getMonetDbConnection().createStatement();)
		{
			copyStmt.execute(query.toString());
		}

	}

}
