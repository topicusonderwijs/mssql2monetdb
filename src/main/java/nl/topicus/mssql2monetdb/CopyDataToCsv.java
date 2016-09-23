package nl.topicus.mssql2monetdb;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CopyDataToCsv extends AbstractCopyData  
{
	private static final Logger LOG =  LoggerFactory.getLogger(CopyDataToCsv.class);
			
	public long copyData (CopyTable copyTable, ResultSetMetaData metaData, ResultSet resultSet, long rowCount) throws SQLException, IOException
	{
		String tmpDir = getConfig().getTempDirectory();
		String tmpFilePrefix = copyTable.getTempFilePrefix();
		
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
				CopyTool.printInsertProgress(startTime, insertCount, rowCount, "written to disk");
			}
		}
		bw.flush();
		bw.close();
		CopyTool.printInsertProgress(startTime, insertCount, rowCount, "written to disk");
		
		return insertCount;
	}

}
