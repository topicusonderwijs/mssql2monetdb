package nl.topicus.mssql2monetdb.copy.binary;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nl.topicus.mssql2monetdb.AbstractCopyData;
import nl.topicus.mssql2monetdb.CopyTable;
import nl.topicus.mssql2monetdb.CopyTool;
import nl.topicus.mssql2monetdb.util.MonetDBUtil;

public class CopyDataToBinary extends AbstractCopyData 
{
	private static final Logger LOG =  LoggerFactory.getLogger(CopyDataToBinary.class);
	
	/**
	 * Methode to check if a table can be loaded with binary mode. This depends on the schema
	 * of the table.
	 * @throws SQLException 
	 */
	public static boolean isBinarySupported (ResultSetMetaData metaData) throws SQLException
	{		
		for (int i = 1; i <= metaData.getColumnCount(); i++)
		{
			if (getConverter(metaData, i) == null)
			{
				String colType = MonetDBUtil.getMonetDbColumnType(i, metaData);
				LOG.info("Column '{}' with type '{}' is not supported in binary load", metaData.getColumnName(i), colType);
				return false;
			}
		}
		
		return true;
	}
	
	private static ValueConverter getConverter (ResultSetMetaData metaData, int colIndex) throws SQLException
	{
		String columnType = MonetDBUtil.getMonetDbColumnType(colIndex, metaData).toLowerCase();
		
		switch(columnType)
		{
			case "int":
			case "integer":
				return new IntegerValueConverter();
			case "bigint":
				return new LongValueConverter();
			case "varchar":
			case "char":
				return new StringValueConverter();
			case "timestamp":
				return new TimestampValueConverter();
			case "double":
				return new DoubleValueConverter();
			case "numeric":
				return new BigDecimalConverter(metaData.getPrecision(colIndex));
		}
		
		return null;
	}
	
	@Override
	public long copyData (CopyTable copyTable, ResultSetMetaData metaData, ResultSet resultSet, long rowCount) throws SQLException, IOException 
	{
		String tmpDir = getConfig().getTempDirectory();
		String tmpFilePrefix = copyTable.getTempFilePrefix();
		
		long startTime = System.currentTimeMillis();
		long insertCount = 0;
		int columnCount = metaData.getColumnCount();
				
		BufferedOutputStream[] bw = new BufferedOutputStream[columnCount];
		ValueConverter[] converters = new ValueConverter[columnCount];
		for (int i = 1; i <= columnCount; i++)
		{
			bw[i-1] = new BufferedOutputStream(new FileOutputStream(tmpDir + "/" + tmpFilePrefix + "_data_" + i + ".bin"));
			converters[i-1] = getConverter(metaData, i);
		}
		
		while (resultSet.next())
		{
			for (int i = 1; i <= columnCount; i++)
			{
				Object value = resultSet.getObject(i);			
				bw[i-1].write(converters[i-1].convertValue(value));
			}

			insertCount++;

			if (insertCount % 100000 == 0)
			{
				for (int i = 1; i <= columnCount; i++)
				{
					bw[i-1].flush();
				}
				
				CopyTool.printInsertProgress(startTime, insertCount, rowCount, "written to disk");
			}
		}
		
		for (int i = 1; i <= columnCount; i++)
		{
			bw[i-1].flush();
			bw[i-1].close();
		}
		
		CopyTool.printInsertProgress(startTime, insertCount, rowCount, "written to disk");
		
		return insertCount;
	}
	
	
	
	
	

}
