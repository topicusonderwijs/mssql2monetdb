package nl.topicus.mssql2monetdb;

/**
 * Types of sourcedatabases supported. 
 * 
 * @author bos
 */
public enum SourceDatabaseType {
	MSSQL
	{
		@Override
		public String toString() {
			return "MS SQL Server";
		}
		
		public String getJDBCUrl(String server, int port, String database) {
			return "jdbc:jtds:sqlserver://" + server + ":" + port + "/" + database;
		}

		@Override
		public String getSelectTriggerColumnQuery(String triggerTable, String triggerColumn) {
			return "SELECT TOP 1 [" + triggerColumn + "] "
					+ "FROM [" + triggerTable + "] "
					+ "ORDER BY [" + triggerColumn + "] DESC";
		}

		@Override
		public int getDefaultPort() {
			return 1433;
		}
	},
	POSTGRESQL
	{
		@Override
		public String toString() {
			return "PostgreSQL";
		}

		@Override
		public String getJDBCUrl(String server, int port, String database) {
			return "jdbc:postgresql://" + server + ":" + port + "/" + database;
		}

		@Override
		public String getSelectTriggerColumnQuery(String triggerTable, String triggerColumn) {
			return "SELECT " + triggerColumn + " "
					+ "FROM " + triggerTable + " "
					+ "ORDER BY " + triggerColumn + " DESC LIMIT 1";
		}

		@Override
		public int getDefaultPort() {
			return 5432;
		}
	};

	/**
	 * 
	 * @param server
	 * @param port
	 * @param database
	 * @return The JDBC URL used for this database type.
	 */
	public abstract String getJDBCUrl(String server, int port, String database);

	/**
	 * 
	 * @param triggerTable
	 * @param triggerColumn
	 * @return The SQL Query used for selecting the triggercolumn.
	 */
	public abstract String getSelectTriggerColumnQuery(String triggerTable, String triggerColumn);

	/**
	 * 
	 * @return The default port for connections to this database.
	 */
	public abstract int getDefaultPort();
}
