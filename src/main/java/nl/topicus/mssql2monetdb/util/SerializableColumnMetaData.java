package nl.topicus.mssql2monetdb.util;

import java.io.Serializable;

public class SerializableColumnMetaData implements Serializable 
{
	private static final long serialVersionUID = 1L;
	
	public boolean isAutoIncrement;
	
	public boolean isCaseSensitive;
	
	public boolean isSearchable;
	
	public boolean isCurrency;
	
	public int isNullable;
	
	public boolean isSigned;
	
	public int columnDisplaySize;
	
	public String columnLabel;
	
	public String columnName;
	
	public String schemaName;
	
	public int precision;
	
	public int scale;
	
	public String tableName;
	
	public String catalogName;
	
	public int columnType;
	
	public String columnTypeName;
	
	public boolean isReadOnly;
	
	public boolean isWritable;
	
	public boolean isDefinitelyWritable;
	
	public String columnClassName;

}
