package nl.topicus.mssql2monetdb.util;

import java.io.Serializable;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * This class is used to persist a live ResultMetaData object to
 * storage.
 * 
 * @author Dennis Pallett <dennis.pallett@topics.nl>
 */
public class SerializableResultSetMetaData implements ResultSetMetaData, Serializable {

	private static final long serialVersionUID = 1L;
	
	private ArrayList<SerializableColumnMetaData> columnList;
	
	public SerializableResultSetMetaData (ResultSetMetaData liveMetaData) throws SQLException
	{
		
		columnList = new ArrayList<SerializableColumnMetaData>(liveMetaData.getColumnCount());
		
		for (int i = 1; i <= liveMetaData.getColumnCount(); i++)
		{
			SerializableColumnMetaData col = new SerializableColumnMetaData();
			
			col.isAutoIncrement = liveMetaData.isAutoIncrement(i);
			col.isCaseSensitive = liveMetaData.isCaseSensitive(i);
			col.isSearchable = liveMetaData.isSearchable(i);
			col.isNullable = liveMetaData.isNullable(i);
			col.isSigned = liveMetaData.isSigned(i);
			col.columnDisplaySize = liveMetaData.getColumnDisplaySize(i);
			col.columnLabel = liveMetaData.getColumnLabel(i);
			col.columnName = liveMetaData.getColumnName(i);
			col.schemaName = liveMetaData.getSchemaName(i);
			col.precision = liveMetaData.getPrecision(i);
			col.scale = liveMetaData.getScale(i);
			col.tableName = liveMetaData.getTableName(i);
			col.catalogName = liveMetaData.getCatalogName(i);
			col.columnType = liveMetaData.getColumnType(i);
			col.columnTypeName = liveMetaData.getColumnTypeName(i);
			col.isReadOnly = liveMetaData.isReadOnly(i);
			col.isWritable = liveMetaData.isWritable(i);
			col.isDefinitelyWritable = liveMetaData.isDefinitelyWritable(i);
			col.columnClassName = liveMetaData.getColumnClassName(i);
			
			columnList.add(col);
		}
	}
	
	public SerializableResultSetMetaData ()
	{
		// empty
	}

	private SerializableColumnMetaData getColumn (int column) throws SQLException
	{
		// column index starts at 1
		// but our internal array starts at 0
		int index = column - 1;
		
		if (columnList == null || index >= columnList.size())
		{
			throw new SQLException("Column index '" + column + "' out-of-bounds!");
		}
		
		return columnList.get(index);
	}
	
	@Override
	public int getColumnCount() throws SQLException {
		return this.columnList.size();
	}

	@Override
	public boolean isAutoIncrement(int column) throws SQLException {
		return getColumn(column).isAutoIncrement;
	}

	@Override
	public boolean isCaseSensitive(int column) throws SQLException {
		return getColumn(column).isCaseSensitive;
	}

	@Override
	public boolean isSearchable(int column) throws SQLException {
		return getColumn(column).isSearchable;
	}

	@Override
	public boolean isCurrency(int column) throws SQLException {
		return getColumn(column).isCurrency;
	}

	@Override
	public int isNullable(int column) throws SQLException {
		return getColumn(column).isNullable;
	}

	@Override
	public boolean isSigned(int column) throws SQLException {
		return getColumn(column).isSigned;
	}

	@Override
	public int getColumnDisplaySize(int column) throws SQLException {
		return getColumn(column).columnDisplaySize;
	}

	@Override
	public String getColumnLabel(int column) throws SQLException {
		return getColumn(column).columnLabel;
	}

	@Override
	public String getColumnName(int column) throws SQLException {
		return getColumn(column).columnName;
	}

	@Override
	public String getSchemaName(int column) throws SQLException {
		return getColumn(column).schemaName;
	}

	@Override
	public int getPrecision(int column) throws SQLException {
		return getColumn(column).precision;
	}

	@Override
	public int getScale(int column) throws SQLException {
		return getColumn(column).scale;
	}

	@Override
	public String getTableName(int column) throws SQLException {
		return getColumn(column).tableName;
	}

	@Override
	public String getCatalogName(int column) throws SQLException {
		return getColumn(column).catalogName;
	}

	@Override
	public int getColumnType(int column) throws SQLException {
		return getColumn(column).columnType;
	}

	@Override
	public String getColumnTypeName(int column) throws SQLException {
		return getColumn(column).columnTypeName;
	}

	@Override
	public boolean isReadOnly(int column) throws SQLException {
		return getColumn(column).isReadOnly;
	}

	@Override
	public boolean isWritable(int column) throws SQLException {
		return getColumn(column).isWritable;
	}

	@Override
	public boolean isDefinitelyWritable(int column) throws SQLException {
		return getColumn(column).isDefinitelyWritable;
	}

	@Override
	public String getColumnClassName(int column) throws SQLException {
		return getColumn(column).columnClassName;
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new UnsupportedOperationException("Not supported");
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new UnsupportedOperationException("Not supported");
	}
}
