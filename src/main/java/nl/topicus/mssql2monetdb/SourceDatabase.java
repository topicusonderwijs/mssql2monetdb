package nl.topicus.mssql2monetdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SourceDatabase {
	private static final Logger LOG = LoggerFactory.getLogger(SourceDatabase.class);
	
	private SourceDatabaseType databaseType;
	
	private String id;
	
	private String server;
	
	private int port = -1;
	
	private String database;

	private String user;
	
	private String password;
	
	private String instance;
	
	private Connection conn;
	
	public Connection getConnection() throws SQLException
	{
		// already have an existing connection?
		if (this.conn != null && !this.conn.isClosed())
		{
			return this.conn;
		}
		
		Properties connProps = new Properties();
		
		if (StringUtils.isEmpty(user) == false && StringUtils.isEmpty(password) == false)
		{
			connProps.setProperty("user", user);
			connProps.setProperty("password", password);
		}

		if (StringUtils.isEmpty(instance) == false)
		{
			connProps.setProperty("instance", instance);
		}
		
		String jdbcUrl = databaseType.getJDBCUrl(server, port, database);
		LOG.info("Using connection URL for " + databaseType + " '" + this.id + "': " + jdbcUrl);

		this.conn = DriverManager.getConnection(jdbcUrl, connProps);
		LOG.info("Opened connection to " + databaseType + " '" + this.id + "'");
		
		return this.conn;
	}
	
	public void closeConnection ()
	{
		if (this.conn == null)
			return;
		
		try {
			if (!this.conn.isClosed())
			{
				this.conn.close();
				LOG.info("Closed connection to " + databaseType + " '" + this.id + "'");
			}
			
		} catch (SQLException e) {
			LOG.warn("Got exception trying to close connection to " + databaseType + " '" + this.id + "'", e);
		}			
	}
	
	
	public SourceDatabaseType getDatabaseType() {
		return databaseType;
	}

	public void setDatabaseType(SourceDatabaseType databaseType) {
		this.databaseType = databaseType;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getServer() {
		return server;
	}

	public void setServer(String host) {
		this.server = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getInstance() {
		return instance;
	}

	public void setInstance(String instance) {
		this.instance = instance;
	}

}
