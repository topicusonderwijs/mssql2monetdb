package nl.topicus.mssql2monetdb.tool;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import nl.cwi.monetdb.mcl.net.MapiSocket;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CopyToolConnectionManager
{
	private static final Logger LOG = LoggerFactory.getLogger(CopyToolConnectionManager.class);

	private static CopyToolConnectionManager instance = null;

	private Connection monetDbConn;

	private MapiSocket monetDbServer;
	
	private Map<String, SourceDatabase> sourceDatabases;
	
	private CopyToolConfig config;

	private CopyToolConnectionManager()
	{
	}

	private synchronized static void createInstance()
	{
		if (instance == null)
			instance = new CopyToolConnectionManager();
	}

	public static CopyToolConnectionManager getInstance()
	{
		if (instance == null)
			createInstance();

		return instance;
	}
	
	public void setConfig(CopyToolConfig config)
	{
		this.config = config;
		this.sourceDatabases = config.getSourceDatabases();
	}
	
	public void openMonetDbConnection () throws SQLException
	{
		Properties databaseProperties = config.getDatabaseProperties();		
	
		if (monetDbConn == null || monetDbConn.isClosed())
		{
			Properties connProps = new Properties();
			String user = databaseProperties.getProperty(CONFIG_KEYS.MONETDB_USER.toString());
			String password =
				databaseProperties.getProperty(CONFIG_KEYS.MONETDB_PASSWORD.toString());

			if (StringUtils.isEmpty(user) == false && StringUtils.isEmpty(password) == false)
			{
				connProps.setProperty("user", user);
				connProps.setProperty("password", password);
			}

			String url =
				"jdbc:monetdb://"
					+ databaseProperties.getProperty(CONFIG_KEYS.MONETDB_SERVER.toString())
					+ "/"
					+ databaseProperties.getProperty(CONFIG_KEYS.MONETDB_DATABASE.toString());
			LOG.info("Using connection URL for MonetDB: " + url);

			monetDbConn = DriverManager.getConnection(url, connProps);
			LOG.info("Opened connection to MonetDB");
		}
	}
	
	public void openMonetDbServerConnection ()
	{
		Properties databaseProperties = config.getDatabaseProperties();		
		
		monetDbServer = new MapiSocket();

		monetDbServer.setDatabase(databaseProperties.getProperty(CONFIG_KEYS.MONETDB_DATABASE
			.toString()));
		monetDbServer.setLanguage("sql");

		try
		{
			LOG.info("Opening direct connection to MonetDB server...");
			List<String> warnList =
				monetDbServer.connect(
					databaseProperties.getProperty(CONFIG_KEYS.MONETDB_SERVER.toString()), 50000,
					databaseProperties.getProperty(CONFIG_KEYS.MONETDB_USER.toString()),
					databaseProperties.getProperty(CONFIG_KEYS.MONETDB_PASSWORD.toString()));

			if (warnList != null && warnList.size() > 0)
			{
				for (String warning : warnList)
				{
					LOG.error(warning);
				}

				LOG.error("Unable to setup direct connection with MonetDB server");
				monetDbServer.close();
				monetDbServer = null;

			}
			else
			{
				LOG.info("Direct connection opened");
			}
		}
		catch (Exception e)
		{
			LOG.error("Unable to setup direct connection with MonetDB server");

			monetDbServer.close();
			monetDbServer = null;
		}
	}

	public void closeConnections()
	{
		LOG.info("Closing database connections...");
		
		// close all connections to source MS SQL databases
		for(SourceDatabase db : sourceDatabases.values())
		{
			db.closeConnection();
		}

		// close connection to target MonetDB database
		try
		{
			if (monetDbConn != null && monetDbConn.isClosed() == false)
			{
				monetDbConn.close();
				LOG.info("Closed JDBC connection to MonetDB server");
			}
		}
		catch (SQLException e)
		{
			// don't care about this exception
			LOG.warn("Unable to close connection to MonetDB server", e);
		}

		if (monetDbServer != null)
		{
			monetDbServer.close();
			LOG.info("Closed direct connection to MonetDB server");
		}
	}

	public Connection getMssqlConnection(String sourceId) throws SQLException
	{
		SourceDatabase db = sourceDatabases.get(sourceId);
		
		if (db == null)
		{
			LOG.warn("Unable to retrieve connection for non-existant source database '" + sourceId + "'");
			return null;
		}
		else
		{
			return db.getConnection();
		}
	}

	public Connection getMonetDbConnection() throws SQLException
	{
		// make sure MonetDB connection is opened
		if (monetDbConn == null || monetDbConn.isClosed())
			openMonetDbConnection();
		
		return monetDbConn;
	}

	public MapiSocket getMonetDbServer()
	{
		if (monetDbServer == null)
			this.openMonetDbServerConnection();
		
		return monetDbServer;
	}

}
