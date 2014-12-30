package nl.topicus.mssql2monetdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public class SourceDatabase {
	private static final Logger LOG = Logger.getLogger(SourceDatabase.class);
	
	private String id;
	
	private String server;
	
	private int port = 1433;
	
	private String database;

	private String user;
	
	private String password;
	
	private String instance;
	
	private Connection conn;
	
	public Connection getConnection () throws SQLException
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
		
		String url = "jdbc:jtds:sqlserver://" + server + ":" + port + "/" + database;
		LOG.info("Using connection URL for MS SQL Server '" + this.id + "': " + url);

		this.conn = DriverManager.getConnection(url, connProps);
		LOG.info("Opened connection to MS SQL Server '" + this.id + "'");
		
		return this.conn;
	}
	
	public void closeConnection ()
	{
		if (this.conn == null)
			return;
		
		try {
			if (!this.conn.isClosed())
				this.conn.close();
			
		} catch (SQLException e) {
			// ignore silently
		}			
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