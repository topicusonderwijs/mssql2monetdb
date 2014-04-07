package nl.topicus.mssql2monetdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import nl.cwi.monetdb.mcl.net.MapiSocket;
import nl.topicus.mssql2monetdb.util.EmailUtil;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public class CopyToolConnectionManager
{
	private static final Logger LOG = Logger.getLogger(CopyToolConnectionManager.class);

	private static CopyToolConnectionManager instance = null;

	private Connection mssqlConn;

	private Connection monetDbConn;

	private MapiSocket monetDbServer;

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

	public void openConnections(Properties databaseProperties)
	{
		// make sure JDBC drivers are loaded
		try
		{
			Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		}
		catch (ClassNotFoundException e)
		{
			LOG.fatal("Unable to load MonetDB JDBC driver");
			EmailUtil.sendMail("Unable to load MonetDB JDBC driverwith the following error: "+ e.getStackTrace(), "Unable to load MonetDB JDBC driver in monetdb", databaseProperties);
			System.exit(1);
		}

		try
		{
			Class.forName("net.sourceforge.jtds.jdbc.Driver");
		}
		catch (ClassNotFoundException e)
		{
			LOG.fatal("Unable to load MS SQL jTDS JDBC driver");
			EmailUtil.sendMail("Unable to load MS SQL jTDS JDBC driver with the following error: "+ e.getStackTrace(), "Unable to load MS SQL jTDS JDBC driver in monetdb", databaseProperties);
			System.exit(1);
		}

		try
		{
			if (mssqlConn == null || mssqlConn.isClosed())
			{
				Properties connProps = new Properties();
				String user = databaseProperties.getProperty(CONFIG_KEYS.MSSQL_USER.toString());
				String password =
					databaseProperties.getProperty(CONFIG_KEYS.MSSQL_PASSWORD.toString());
				String instance =
					databaseProperties.getProperty(CONFIG_KEYS.MSSQL_INSTANCE.toString());

				if (StringUtils.isEmpty(user) == false && StringUtils.isEmpty(password) == false)
				{
					connProps.setProperty("user", user);
					connProps.setProperty("password", password);
				}

				if (StringUtils.isEmpty(instance) == false)
				{
					connProps.setProperty("instance", instance);
				}

				String url =
					"jdbc:jtds:sqlserver://"
						+ databaseProperties.getProperty(CONFIG_KEYS.MSSQL_SERVER.toString()) + "/"
						+ databaseProperties.getProperty(CONFIG_KEYS.MSSQL_DATABASE.toString());
				LOG.info("Using connection URL for MS SQL Server: " + url);

				mssqlConn = DriverManager.getConnection(url, connProps);
				LOG.info("Opened connection to MS SQL Server");
			}
		}
		catch (SQLException e)
		{
			LOG.fatal("Unable to open connection to MS SQL server", e);
			EmailUtil.sendMail("Unable to open connection to MS SQL server with the following error: "+ e.getStackTrace(), "Unable to open connection to MS SQL server in monetdb", databaseProperties);
			System.exit(1);
		}

		try
		{
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
				LOG.info("Using connection URL for MonetDB Server: " + url);

				monetDbConn = DriverManager.getConnection(url, connProps);
				LOG.info("Opened connection to MonetDB Server");
			}
		}
		catch (SQLException e)
		{
			LOG.fatal("Unable to open connection to MonetDB server", e);
			closeConnections();
			System.exit(1);
		}

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

		try
		{
			if (mssqlConn != null && mssqlConn.isClosed() == false)
			{
				mssqlConn.close();
				LOG.info("Closed connection to MS SQL server");
			}
		}
		catch (SQLException e)
		{
			// don't care about this exception
			LOG.warn("Unable to close connection to MS SQL server", e);
		}

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

	public Connection getMssqlConnection()
	{
		return mssqlConn;
	}

	public Connection getMonetDbConnection()
	{
		return monetDbConn;
	}

	public MapiSocket getMonetDbServer()
	{
		return monetDbServer;
	}

}
