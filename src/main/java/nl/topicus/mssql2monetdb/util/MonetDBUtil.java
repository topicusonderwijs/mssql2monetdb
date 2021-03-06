package nl.topicus.mssql2monetdb.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nl.topicus.mssql2monetdb.CopyTable;
import nl.topicus.mssql2monetdb.CopyToolConnectionManager;
import nl.topicus.mssql2monetdb.MonetDBTable;

public class MonetDBUtil
{
	private static final Logger LOG = LoggerFactory.getLogger(MonetDBUtil.class);

	/**
	 * Check if a table exists in MonetDB by selecting and catching the
	 * {@link SQLException} if table doesn't exist.
	 */
	public static boolean monetDBTableExists(MonetDBTable monetDBTable) throws SQLException
	{
		return tableOrViewExists(monetDBTable.getCopyTable().getSchema(),
			monetDBTable.getNameWithPrefixes());
	}

	/**
	 * Checks if table (or view) exists by quering the sys.tables table. For that we need
	 * the schema and the name.
	 * 
	 * @param schema
	 *            the schema of the table
	 * @param name
	 *            the name of the table
	 */
	public static boolean tableOrViewExists(String schema, String name) throws SQLException
	{
		try (Statement q =
				CopyToolConnectionManager.getInstance().getMonetDbConnection().createStatement())
		{
			ResultSet result =
				q.executeQuery("SELECT name FROM sys.tables WHERE name = " + quoteMonetDbValue(name)
					+ " AND schema_id = (SELECT id FROM sys.schemas WHERE LOWER(name) = LOWER(" + quoteMonetDbValue(schema)
					+ "))");
			
			boolean ret = result.next();			
			result.close();
			
			// is true if rows exists, otherwise it is false
			return ret;
		}
		catch (SQLException e)
		{
			LOG.error("Error while checking if table or view '" + schema + "." + name + "' exists",
				e);
			throw e;
		}
	}

	public static boolean isTable(String schema, String name) throws SQLException
	{
		try (	Statement q =
				CopyToolConnectionManager.getInstance().getMonetDbConnection().createStatement())
		{
		
			ResultSet result =
				q.executeQuery("SELECT query FROM sys.tables WHERE name = '" + name
					+ "' and schema_id = (SELECT id from sys.schemas WHERE LOWER(name) = LOWER('" + schema
					+ "'))");
			
			// the query column will be filled with a query when its a view, so if its
			// null it will be a table			
			boolean isTable = (result.next() && result.getObject("query") == null);
			result.close();
			
			return isTable;
		}
		catch (SQLException e)
		{
			LOG.error("Error while checking if '" + name + "' is a table", e);
			throw e;
		}
	}

	/**
	 * Truncate MonetDBTable.
	 */
	public static void truncateMonetDBTable(MonetDBTable monetDBTable) throws SQLException
	{
		if (monetDBTableExists(monetDBTable))
		{
			LOG.info("Truncating table '" + monetDBTable.getToTableSql() + "' on MonetDB server...");
			CopyToolConnectionManager.getInstance().getMonetDbConnection().createStatement()
				.execute("DELETE FROM " + monetDBTable.getToTableSql());
			LOG.info("Table truncated");
		}
		else
		{
			LOG.warn("Did not truncate '" + monetDBTable.getToTableSql()
				+ "' because it did not exist.");
		}
	}

	/**
	 * Drop MonetDB table.
	 */
	public static void dropMonetDBTable(MonetDBTable monetDBTable) throws SQLException
	{
		if (monetDBTableExists(monetDBTable)
			&& isTable(monetDBTable.getCopyTable().getSchema(), monetDBTable.getNameWithPrefixes()))
		{
			LOG.info("Dropping table '" + monetDBTable.getToTableSql() + "' in MonetDB database...");
			CopyToolConnectionManager.getInstance().getMonetDbConnection().createStatement()
				.executeUpdate("DROP TABLE " + monetDBTable.getToTableSql());
			LOG.info("Table " + monetDBTable.getToTableSql() + " dropped");
		}
		else
		{
			LOG.warn("Did not drop '" + monetDBTable.getToTableSql()
				+ "' because it did not exist or it isn't a table but a view.");
		}
	}

	/**
	 * Copy (create and insert data) a {@link MonetDBTable} to another
	 * {@link MonetDBTable} with the following statement: CREATE TABLE newTable AS SELECT
	 * * FROM existingTable WITH DATA;
	 * 
	 * @throws SQLException
	 */
	public static void copyMonetDBTableToNewMonetDBTable(MonetDBTable existingTable,
			MonetDBTable newTable) throws SQLException
	{
		if (monetDBTableExists(existingTable))
		{
			LOG.info("Copying table '" + existingTable.getToTableSql() + "' to '"
				+ newTable.getToTableSql() + "' with data");
			CopyToolConnectionManager
				.getInstance()
				.getMonetDbConnection()
				.createStatement()
				.execute(
					"CREATE TABLE " + newTable.getToTableSql() + " AS SELECT * FROM "
						+ existingTable.getToTableSql() + " WITH DATA");
		}
		else
		{
			LOG.warn("Did not copy '" + existingTable.getToTableSql() + "' to '"
				+ newTable.getToTableSql() + "', because" + existingTable.getToTableSql()
				+ " did not exist.");
		}
	}

	/**
	 * Create a table in MonetDB based on given table meta data.
	 */
	public static void createMonetDBTable(MonetDBTable monetDBTable, ResultSetMetaData metaData)
			throws SQLException
	{
		// build SQL query to create table
		LOG.info("Creating table '" + monetDBTable.getToTableSql() + "' on MonetDB server...");
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
		CopyToolConnectionManager.getInstance().getMonetDbConnection().createStatement()
			.execute(createSql.toString());
		LOG.info("Table '" + monetDBTable.getToTableSql() + "' created");

		// fresh table so we can use COPY INTO since we know its ok
		// we only do this if the copy method has not already been explicitly set
		CopyTable copyTable = monetDBTable.getCopyTable();
		if (copyTable.getCopyMethod() == CopyTable.COPY_METHOD_NOTSET)
		{
			monetDBTable.getCopyTable().setCopyMethod(CopyTable.COPY_METHOD_COPYINTO);
		}
	}

	/**
	 * Creates a SQL command for creating a column based on table meta data and column
	 * index.
	 */
	public static String createColumnSql(int colIndex, ResultSetMetaData metaData)
			throws SQLException
	{
		StringBuilder createSql = new StringBuilder();

		createSql.append(MonetDBUtil.quoteMonetDbIdentifier(metaData.getColumnName(colIndex)
			.toLowerCase()));
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
		sqlTypes.put(Types.BIT, "boolean");

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

	/**
	 * Make the identifier MonetDB-proof by making lowercase and removing special
	 * characters.
	 */
	public static String prepareMonetDbIdentifier(String ident)
	{
		// MonetDB only supports lowercase identifiers
		ident = ident.toLowerCase();

		// MonetDB doesn't support any special characters so replace with underscore
		ident = ident.replaceAll("[^a-zA-Z0-9]+", "_");

		return ident;
	}

	/**
	 * Put escaped quotes around a value.
	 */
	public static String quoteMonetDbValue(String value)
	{
		return "'" + value.replaceAll("\\\\", "\\\\\\\\").replaceAll("'", "\\\\'") + "'";
	}

	/**
	 * Quote and make the identifier MonetDB-proof by making lowercase and removing
	 * special characters.
	 */
	public static String quoteMonetDbIdentifier(String ident)
	{
		// prepare identifier
		ident = prepareMonetDbIdentifier(ident);

		// make sure identifier is actually quoted
		ident = "\"" + ident + "\"";

		return ident;
	}

	/**
	 * Verify if a {@link MonetDBTable} is conform the given table meta data. And if
	 * columns are missing, add them.
	 */
	public static void verifyColumnsOfExistingTable(MonetDBTable table, ResultSetMetaData metaData)
			throws SQLException
	{
		LOG.info("Verifying existing table '" + table.getToTableSql()
			+ "' in MonetDB matches table schema in MS SQL...");

		// do a select on the table in MonetDB to get its metadata
		Statement q =
			CopyToolConnectionManager.getInstance().getMonetDbConnection().createStatement();
		ResultSet res = q.executeQuery("SELECT * FROM " + table.getToTableSql() + " LIMIT 1");
		ResultSetMetaData monetDbMetaData = res.getMetaData();

		// create a mapping of MonetDB columns and related column indexes
		HashMap<String, Integer> colMapping = new HashMap<String, Integer>();
		for (int i = 1; i <= monetDbMetaData.getColumnCount(); i++)
		{
			String colName = monetDbMetaData.getColumnName(i);
			colMapping.put(MonetDBUtil.prepareMonetDbIdentifier(colName), i);
		}

		// loop through columns of MS SQL and verify with columns in MonetDB
		for (int i = 1; i <= metaData.getColumnCount(); i++)
		{
			String colName = MonetDBUtil.prepareMonetDbIdentifier(metaData.getColumnName(i));

			// col name exists in MonetDB?
			if (colMapping.containsKey(colName))
			{
				// verify type
				// TODO: actually verify type
			}
			else
			{
				// create column in MonetDB
				LOG.info("Column '" + colName + "' is missing in MonetDB table");
				LOG.info("Adding column '" + colName + "' in table " + table.getToTableSql()
					+ " in MonetDB...");

				String sql =
					"ALTER TABLE " + table.getToTableSql() + " ADD COLUMN "
						+ MonetDBUtil.createColumnSql(i, metaData);
				Statement createColumn =
					CopyToolConnectionManager.getInstance().getMonetDbConnection()
						.createStatement();
				createColumn.execute(sql);

				LOG.info("Column '" + colName + "'added");
			}
		}

		// close objects
		res.close();
		q.close();

		LOG.info("Table verified");
	}

	public static void dropMonetDBTableOrView (MonetDBTable monetDBTable) throws SQLException
	{
		String schema = monetDBTable.getCopyTable().getSchema();
		String name = monetDBTable.getName();
		
		String fullName = schema + "." + name;
		if (monetDBTableExists(monetDBTable))
		{
			LOG.info("Dropping table or view '" + fullName + "' on MonetDB server...");
			
			try (Statement stmt =
					CopyToolConnectionManager.getInstance().getMonetDbConnection()
						.createStatement())
			{
				StringBuilder builder = new StringBuilder();
				if (tableOrViewExists(schema, name))
				{
					// if its a table, drop the table
					if (isTable(schema, name))
						builder.append("DROP TABLE " + fullName + ";");
					else
						builder.append("DROP VIEW " + fullName + ";");
				}
				
				stmt.execute(builder.toString());
			}
			catch (SQLException e)
			{
				LOG.error("Error dropping '" + fullName + "'", e);
				throw new RuntimeException(e);
			}
			
			LOG.info("Table or view '" + fullName + "' dropped");
		}
	}
	
	/**
	 * Drops a view, if it exists, and creates the view for which queries (select * from)
	 * a {@link MonetDBTable}.
	 * 
	 * @param name
	 *            the name of the view we need to create
	 * @param schema
	 *            the schema of the view, we need this separately for the name to check if
	 *            the table exists
	 */
	public static void dropAndRecreateViewForTable(String schema, String name,
			MonetDBTable monetDBTable) throws SQLException
	{
		Connection conn = CopyToolConnectionManager.getInstance().getMonetDbConnection();
		
		// disable autocommit to work in a single transaction
		boolean oldAutoCommit = conn.getAutoCommit();
		conn.setAutoCommit(false);
		
		// make sure the referenced table exists 
		if (monetDBTableExists(monetDBTable))
		{
			String fullName = schema + "." + name;
			
			LOG.info("Drop and recreate or create the view '" + fullName + "' for table '"
				+ monetDBTable.getToTableSql() + "' on MonetDB server...");
			
			// drop current table/view
			// we do this in a loop (of max 10) to ensure it really is dropped
			// due to a possible bug in MonetDB whereby a view can exist multiple times
			for(int i=0; i < 10 && tableOrViewExists(schema, name); i++)
			{				
				boolean isTable = isTable(schema, name);
				
				// display warning when table/view should have been dropped already
				if (i > 0)
				{
					if (isTable)
						LOG.warn(String.format("Table %s still exists despite previous DROP. This should not be possible!", fullName));
					else
						LOG.warn(String.format("View %s still exists despite previous DROP. This should not be possible!", fullName));
				}
				
				try (Statement stmt = conn.createStatement()) 
				{
					if (isTable)
					{
						LOG.info(String.format("Dropping existing table '%s'...", fullName));
						stmt.execute("DROP TABLE " + fullName + ";");
						LOG.info("Table dropped");
					}
					else
					{
						LOG.info(String.format("Dropping existing view '%s'...", fullName));
						stmt.execute("DROP VIEW " + fullName + ";");
						LOG.info("View dropped");
					}
				} catch (SQLException e) {
					if (isTable)
						LOG.error(String.format("Unable to drop table '%s'", fullName));
					else
						LOG.error(String.format("Unable to drop view '%s'", fullName));
					
					throw e;
				}
			}
			
			// create new view
			try (Statement q = conn.createStatement())
			{
				q.execute(String.format(
					"CREATE VIEW %s AS SELECT * FROM %s",
					fullName,
					monetDBTable.getToTableSql()
				));
			} catch (SQLException e) {
				LOG.error(String.format(
					"Unable to create view %s for table %s",
					fullName,
					monetDBTable.getToTableSql()
				));				
				
				throw e;
			}
			
			// commit transaction
			conn.commit();
			LOG.info("View '" + fullName + "' created");
		}
		else
		{
			LOG.info("View not created because the MonetDB table '" + monetDBTable.getToTableSql()
				+ "' does not exist");
		}
		
		// restore previous autocommit mode
		conn.setAutoCommit(oldAutoCommit);
	}
}
