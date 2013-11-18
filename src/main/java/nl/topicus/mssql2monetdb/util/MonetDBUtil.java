package nl.topicus.mssql2monetdb.util;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;

import nl.topicus.mssql2monetdb.CopyTable;
import nl.topicus.mssql2monetdb.CopyToolConnectionManager;
import nl.topicus.mssql2monetdb.MonetDBTable;

import org.apache.log4j.Logger;

public class MonetDBUtil
{
	private static final Logger LOG = Logger.getLogger(MonetDBUtil.class);

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
		try
		{
			Statement q =
				CopyToolConnectionManager.getInstance().getMonetDbConnection().createStatement();
			ResultSet result =
				q.executeQuery("SELECT name FROM sys.tables WHERE name = '" + name
					+ "' AND schema_id = (SELECT id FROM sys.schemas WHERE name = '" + schema
					+ "')");
			// is true if rows exists, otherwise it is false
			return result.next();
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
		try
		{
			Statement q =
				CopyToolConnectionManager.getInstance().getMonetDbConnection().createStatement();
			ResultSet result =
				q.executeQuery("SELECT query FROM sys.tables WHERE name = '" + name
					+ "' and schema_id = (SELECT id from sys.schemas WHERE name = '" + schema
					+ "')");
			// move resultset to the actual result
			if (!result.next())
				return false;
			// the query column will be filled with a query when its a view, so if its
			// null it will be a table
			return result.getObject("query") == null;
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
			LOG.info("Truncating table " + monetDBTable.getToTableSql() + " on MonetDB server...");
			CopyToolConnectionManager.getInstance().getMonetDbConnection().createStatement()
				.execute("DELETE FROM " + monetDBTable.getToTableSql());
			LOG.info("Table truncated");
		}
		else
		{
			LOG.warn("Did not truncate " + monetDBTable.getToTableSql()
				+ " because it did not exist.");
		}
	}

	/**
	 * Drop MonetDB table.
	 */
	public static void dropMonetDBTable(MonetDBTable monetDBTable) throws SQLException
	{
		if (monetDBTableExists(monetDBTable))
		{
			LOG.info("Dropping table " + monetDBTable.getToTableSql() + " in MonetDB database...");
			CopyToolConnectionManager.getInstance().getMonetDbConnection().createStatement()
				.executeUpdate("DROP TABLE " + monetDBTable.getToTableSql());
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
	public static void copyMonetDBTableToNewMonetDBTable(MonetDBTable existingTable,
			MonetDBTable newTable) throws SQLException
	{
		if (monetDBTableExists(existingTable))
		{
			LOG.info("Copying table " + existingTable.getToTableSql() + " to "
				+ newTable.getToTableSql() + " with data");
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
			LOG.warn("Did not copy " + existingTable.getToTableSql() + " to "
				+ newTable.getToTableSql() + ", because" + existingTable.getToTableSql()
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
		CopyToolConnectionManager.getInstance().getMonetDbConnection().createStatement()
			.execute(createSql.toString());
		LOG.info("Table created");

		// fresh table so we can use COPY INTO since we know its ok
		monetDBTable.getCopyTable().setCopyMethod(CopyTable.COPY_METHOD_COPYINTO);
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
		LOG.info("Verifying existing table " + table.getToTableSql()
			+ " in MonetDB matches table schema in MS SQL...");

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
				LOG.info("Column " + colName + " is missing in MonetDB table");
				LOG.info("Adding column " + colName + " in table " + table.getToTableSql()
					+ " in MonetDB...");

				String sql =
					"ALTER TABLE " + table.getToTableSql() + " ADD COLUMN "
						+ MonetDBUtil.createColumnSql(i, metaData);
				Statement createColumn =
					CopyToolConnectionManager.getInstance().getMonetDbConnection()
						.createStatement();
				createColumn.execute(sql);

				LOG.info("Column added");
			}
		}

		// close objects
		res.close();
		q.close();

		LOG.info("Table verified");
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
		String fullName = schema + "." + name;
		if (monetDBTableExists(monetDBTable))
		{
			LOG.info("Creating view " + name + " on MonetDB server...");

			// execute CREATE TABLE SQL query
			try
			{
				Statement stmt =
					CopyToolConnectionManager.getInstance().getMonetDbConnection()
						.createStatement();
				StringBuilder builder = new StringBuilder();
				if (tableOrViewExists(schema, name))
				{
					// if its a table, drop the table
					if (isTable(schema, name))
						builder.append("DROP TABLE " + fullName + ";");
					else
						builder.append("DROP VIEW " + fullName + ";");
				}

				builder.append("CREATE VIEW " + fullName + " AS SELECT * FROM "
					+ monetDBTable.getToTableSql());

				stmt.execute(builder.toString());
			}
			catch (SQLException e)
			{
				LOG.error("Error dropping and recreating the view "
					+ monetDBTable.getCopyTable().getToName(), e);
				throw new RuntimeException(e);
			}
			LOG.info("View created");

			// fresh table so we can use COPY INTO since we know its ok
			monetDBTable.getCopyTable().setCopyMethod(CopyTable.COPY_METHOD_COPYINTO);
		}
		else
		{
			LOG.info("View not created because the monetDBTable " + monetDBTable.getToTableSql()
				+ " does not exist");
		}
	}
}
