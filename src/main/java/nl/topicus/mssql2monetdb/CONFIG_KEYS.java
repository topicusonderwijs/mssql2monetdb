package nl.topicus.mssql2monetdb;

public enum CONFIG_KEYS
{
	MSSQL_USER("mssql.user"),
	MSSQL_PASSWORD("mssql.password"),
	MSSQL_SERVER("mssql.server"),
	MSSQL_DATABASE("mssql.database"),
	MSSQL_INSTANCE("mssql.instance"),
	MONETDB_USER("monetdb.user"),
	MONETDB_PASSWORD("monetdb.password"),
	MONETDB_SERVER("monetdb.server"),
	MONETDB_DATABASE("monetdb.database"),
	BATCH_SIZE("batch.size", false);

	private final String key;

	private final boolean required;

	private CONFIG_KEYS(final String key, final boolean required)
	{
		this.key = key;
		this.required = required;
	}

	private CONFIG_KEYS(final String key)
	{
		this(key, false);
	}

	public boolean isRequired()
	{
		return this.required;
	}

	@Override
	public String toString()
	{
		return this.key;
	}

}
