package nl.topicus.mssql2monetdb;

public enum CONFIG_KEYS
{
	JOB_ID("job.id"),
	MONETDB_USER("monetdb.user"),
	MONETDB_PASSWORD("monetdb.password"),
	MONETDB_SERVER("monetdb.server"),
	MONETDB_DATABASE("monetdb.database"),
	MAIL_TO("mail.to"),
	MAIL_FROM("mail.from"),
	MAIL_SERVER("mail.server"),
	MAIL_PORT("mail.port"),
	MAIL_USERNAME("mail.username"),
	MAIL_PASSWORD("mail.password"),
	MAIL_SUBJECT("mail.subject"),
	SCHEDULER_ENABLED("scheduler.enabled", false),
	SCHEDULER_INTERVAL("scheduler.interval", false),
	TRIGGER_ENABLED("trigger.enabled", false),
	TRIGGER_SOURCE("trigger.source", false),
	TRIGGER_TABLE("trigger.table", false),
	TRIGGER_COLUMN("trigger.column", false),
	TRIGGER_DIR("trigger.directory", false),
	BATCH_SIZE("batch.size", false),
	TEMP_DIR("temp.directory", false);
	
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
