package nl.topicus.mssql2monetdb.util;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import nl.topicus.mssql2monetdb.CopyTable;
import nl.topicus.mssql2monetdb.CopyToolConnectionManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SourceDatabaseUtil
{
	private static final Logger LOG = LoggerFactory.getLogger(SourceDatabaseUtil.class);

	/**
	 * Check if all the source tables we are copying from have data. If a table is empty,
	 * this usually indicates a problem, so we stop all copy actions.
	 */
	public static boolean allSourceTablesHaveData(Map<String, CopyTable> tablesToCopy) throws SQLException
	{
		for (CopyTable table : tablesToCopy.values())
		{
			// select data from MS SQL Server
			Statement selectStmt =
				CopyToolConnectionManager.getInstance().getSourceConnection(table.getSource()).createStatement();
			// get number of rows in table
			ResultSet resultSet =
				selectStmt.executeQuery(table.generateCountQuery());
			resultSet.next();
			long rowCount = resultSet.getLong(1);
			resultSet.close();

			if (rowCount == 0)
			{
				if (table.isAllowEmpty())
				{
					LOG.warn("Soruce tabe {} is empty", table.getDescription());
				}
				else
				{
					LOG.error("Source table {} is empty! Stopping all copy actions!", table.getDescription());
					return false;
				}
			}
		}
	

		return true;
	}
}
