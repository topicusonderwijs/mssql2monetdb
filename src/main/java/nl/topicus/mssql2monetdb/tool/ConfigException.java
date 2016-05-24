package nl.topicus.mssql2monetdb.tool;

public class ConfigException extends Exception {
	private static final long serialVersionUID = 1L;

	public ConfigException (String msg) {
		super(msg);
	}

	public ConfigException (String msg, Object... args) {
		this(String.format(msg, args));
	}
}
