package nl.topicus.mssql2monetdb;

public enum CONFIG_KEYS
{
	JOB_ID("job.id"),
	MONETDB_USER("monetdb.user"),
	MONETDB_PASSWORD("monetdb.password"),
	MONETDB_SERVER("monetdb.server"),
	MONETDB_DATABASE("monetdb.database"),
	MONETDB_MAIL_SENDMAIL("monetdb.mail.sendmail"),
	MONETDB_MAIL_TO("monetdb.mail.to"),
	MONETDB_MAIL_FOM("monetdb.mail.from"),
	MONETDB_MAIL_SERVER("monetdb.mail.server"),
	MONETDB_MAIL_PORT("monetdb.mail.port"),
	MONETDB_MAIL_USERNAME("monetdb.mail.username"),
	MONETDB_MAIL_PASSWORD("monetdb.mail.password"),
	ALLOW_MULTIPLE_INSTANCES("allow.multiple.instances", false),
	SCHEDULER_SOURCE("scheduler.source", false),
	SCHEDULER_TABLE("scheduler.table", false),
	SCHEDULER_COLUMN("scheduler.column", false),
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
