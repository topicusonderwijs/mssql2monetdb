package nl.topicus.mssql2monetdb;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;

import nl.topicus.mssql2monetdb.util.EmailUtil;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class CopyToolConfig
{
	private static final Logger LOG = Logger.getLogger(CopyToolConfig.class);

	public static final int DEFAULT_BATCH_SIZE = 10000;
	
	public static final String DEFAULT_SOURCE_ID = "_default";

	private Properties databaseProperties;

	private int batchSize = DEFAULT_BATCH_SIZE;
	
	private String jobId;
	
	private String schedulerSource;
	
	private String schedulerTable;
	
	private String schedulerColumn;
	
	private boolean allowMultipleInstances = false;
	
	private File configFile;

	private HashMap<String, SourceDatabase> sourceDatabases = new HashMap<String, SourceDatabase>(); 
	
	private HashMap<String, CopyTable> tablesToCopy = new HashMap<String, CopyTable>();
		
	public static String sha1Checksum (File file) throws NoSuchAlgorithmException, IOException
	{
		MessageDigest md = MessageDigest.getInstance("SHA1");
	    FileInputStream fis = new FileInputStream(file);
	    byte[] dataBytes = new byte[1024];
	 
	    int nread = 0; 
	 
	    while ((nread = fis.read(dataBytes)) != -1) {
	      md.update(dataBytes, 0, nread);
	    };
	 
	    byte[] mdbytes = md.digest();
	 
	    //convert the byte to hex format
	    StringBuffer sb = new StringBuffer("");
	    for (int i = 0; i < mdbytes.length; i++) {
	    	sb.append(Integer.toString((mdbytes[i] & 0xff) + 0x100, 16).substring(1));
	    }
	    
	    return sb.toString();
	}

	public CopyToolConfig(String args[])
	{
		PropertyConfigurator.configure("log4j.properties");
		LOG.info("Started logging of the MSSQL2MonetDB copy tool");

		Options options = new Options();

		OptionBuilder.hasArg(true);
		OptionBuilder.isRequired(true);
		OptionBuilder.withDescription("Specify the configuration properties file");
		OptionBuilder.withLongOpt("config");
		options.addOption(OptionBuilder.create("c"));

		CommandLineParser parser = new BasicParser();
		CommandLine cmd = null;
		try
		{
			cmd = parser.parse(options, args);
		}
		catch (ParseException e)
		{
			LOG.error("ERROR: " + e.getMessage());

			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("mssql2monetdb", options);

			System.exit(1);
		}
		if (cmd == null)
		{
			LOG.error("CommandLine parser is null");
			return;
		}

		configFile = new File(cmd.getOptionValue("config"));
		LOG.info("Using config file: " + configFile.getAbsolutePath());

		Properties config = new Properties();
		try
		{
			config.load(new FileInputStream(configFile));
		}
		catch (Exception e)
		{
			LOG.error("ERROR: unable to read config file");
			e.printStackTrace();
			System.exit(1);
		}

		this.databaseProperties = getAndValidateDatabaseProperties(config);
		this.sourceDatabases = findSourceDatabases(config);
		this.tablesToCopy = findTablesToCopy(config);
		
		// verify scheduling source
		checkSchedulingSource();
	}
	
	private void checkSchedulingSource ()
	{
		if (this.isSchedulingEnabled())
		{
			if (StringUtils.isEmpty(this.schedulerSource))
			{
				if (!this.sourceDatabases.containsKey(DEFAULT_SOURCE_ID))
				{
					LOG.warn("No source database has been specified for scheduling table/column and default source does not exist. "
							+ "Scheduling disabled!");
					this.schedulerTable = "";
				}
				else
				{
					this.schedulerSource = DEFAULT_SOURCE_ID;
				}
			}
			else
			{
				if (!this.sourceDatabases.containsKey(this.schedulerSource))
				{
					LOG.warn("Invalid source database '" + this.schedulerSource + "' specified for scheduling. Disabled scheduling!");
					this.schedulerTable = "";
				}
			}

		}
	}

	private Properties getAndValidateDatabaseProperties(Properties config)
	{
		boolean isMissing = false;
		ArrayList<String> missingKeys = new ArrayList<String>();

		for (CONFIG_KEYS key : CONFIG_KEYS.values())
		{
			String value = config.getProperty(key.toString());
			if (key.isRequired() && StringUtils.isEmpty(value))
			{
				isMissing = true;
				LOG.error("Missing config property: " + key);
				missingKeys.add(key.toString());
			}
		}

		if (isMissing)
		{
			LOG.fatal("Missing essential config properties");
			EmailUtil.sendMail("The following configs are missing: " + missingKeys.toString(), "Missing essential config properties in monetdb", config);
			System.exit(1);
		}
		
		jobId = config.getProperty(CONFIG_KEYS.JOB_ID.toString());
		
		// check if scheduler has been specified
		schedulerSource = config.getProperty(CONFIG_KEYS.SCHEDULER_SOURCE.toString());
		schedulerTable = config.getProperty(CONFIG_KEYS.SCHEDULER_TABLE.toString());
		schedulerColumn = config.getProperty(CONFIG_KEYS.SCHEDULER_COLUMN.toString());
		
		// check if multiple instances of the same job can be running concurrently
		String allowMultipleInstancesStr = config.getProperty(CONFIG_KEYS.ALLOW_MULTIPLE_INSTANCES.toString());
		if (!StringUtils.isEmpty(allowMultipleInstancesStr))
		{
			allowMultipleInstancesStr = allowMultipleInstancesStr.toLowerCase();		
			allowMultipleInstances = (allowMultipleInstancesStr.equals("yes") || allowMultipleInstancesStr.equals("true"));
		}

		// check if batch size has been specified
		String batchSizeStr = config.getProperty(CONFIG_KEYS.BATCH_SIZE.toString());
		if (StringUtils.isEmpty(batchSizeStr) == false)
		{
			try
			{
				this.batchSize = Integer.parseInt(batchSizeStr);
			}
			catch (NumberFormatException e)
			{
				// don't care, just ignore
			}
		}

		return config;
	}
	
	private HashMap<String, SourceDatabase> findSourceDatabases (Properties config)
	{
		HashMap<String, SourceDatabase> sourceDatabases = new HashMap<String, SourceDatabase>();
		
		for (Entry<Object, Object> entry : config.entrySet())
		{
			String propName = entry.getKey().toString().toLowerCase();
			String propValue = entry.getValue().toString().trim();
			
			String[] split = propName.split("\\.");
			
			if (split[0].equals("mssql") == false)
				continue;
						
			String id;
			String key;
			if (split.length == 3)
			{
				id = split[1];
				key = split[2];
			} 
			else if (split.length == 2)
			{
				id = DEFAULT_SOURCE_ID;
				key = split[1];
			}
			else
			{
				continue;
			}

			key = key.toLowerCase().trim();
			
			SourceDatabase db = sourceDatabases.get(id);
			
			if (db == null)
			{
				db = new SourceDatabase();
				db.setId(id);
			}
			
			if (key.equals("user"))
			{
				db.setUser(propValue);
			}
			else if (key.equals("password"))
			{
				db.setPassword(propValue);
			}
			else if (key.equals("server"))
			{
				db.setServer(propValue);
			}
			else if (key.equals("database"))
			{
				db.setDatabase(propValue);
			}
			else if (key.equals("instance"))
			{
				db.setInstance(propValue);
			}
			else if (key.equals("port"))
			{
				try 
				{
					int portInt = Integer.parseInt(propValue);
					db.setPort(portInt);
				}
				catch (NumberFormatException e)
				{
					LOG.warn("Invalid port specified for MSSQL '" + id + "', must be a valid integer!");
				}
			}
			
			sourceDatabases.put(id, db);
		}
		
		// verify each source database has a database and server specified
		for(String id : sourceDatabases.keySet())
		{
			SourceDatabase db = sourceDatabases.get(id);
			
			if (StringUtils.isEmpty(db.getDatabase()))
			{
				LOG.warn("MSSQL database with id '" + id + "' is missing the database name in the config!");
			}
			
			if (StringUtils.isEmpty(db.getServer()))
			{
				LOG.warn("MSSQL database with id '" + id + "' is missing the server in the config!");
			}				
		}
		
		if (sourceDatabases.size() == 0)
		{
			LOG.error("Configuration has specified NO source databases!");
			EmailUtil.sendMail("Configuration has specified NO source databases!", "Configuration has specified NO source databases", config);
		}
		else
		{
			LOG.info("The following databases will be used as sources: ");
			for (SourceDatabase db : sourceDatabases.values())
			{
				LOG.info("* " + db.getId() + ": " + db.getDatabase() + " (" + db.getServer() + ")");
			}
		}
		
		return sourceDatabases;
	}

	private HashMap<String, CopyTable> findTablesToCopy(Properties config)
	{
		HashMap<String, CopyTable> tablesToCopy = new HashMap<String, CopyTable>();
		for (Entry<Object, Object> entry : config.entrySet())
		{
			String propName = entry.getKey().toString().toLowerCase();
			String propValue = entry.getValue().toString();
			boolean boolValue =
				(propValue.equalsIgnoreCase("true") || propValue.equalsIgnoreCase("yes"));

			String[] split = propName.split("\\.");

			if (split.length != 3)
				continue;

			if (split[0].equals("table") == false)
				continue;

			String id = split[1];
			String key = split[2].toLowerCase();

			CopyTable table = tablesToCopy.get(id);
			
			// if table does not exist than add new CopyTable with a MonetDBTable
			if (table == null)
			{
				table = new CopyTable();
				table.getMonetDBTables().add(new MonetDBTable(table));
			}

			if (key.equals("source"))
			{
				table.setSource(propValue);
			}
			else if (key.equals("from"))
			{
				table.setFromName(propValue);
			}
			else if (key.equals("to"))
			{
				table.setToName(propValue.toLowerCase());
				table.getCurrentTable().setName(propValue.toLowerCase());
			}
			else if (key.equals("schema"))
			{
				table.setSchema(propValue);
			}
			else if (key.equals("create"))
			{
				table.setCreate(boolValue);
			}
			else if (key.equals("truncate"))
			{
				table.setTruncate(boolValue);
			}
			else if (key.equals("drop"))
			{
				table.setDrop(boolValue);
			}
			else if (key.equals("copyviatemptable"))
			{
				table.setCopyViaTempTable(boolValue);
			}
			else if (key.equals("temptableprefix"))
			{
				table.setTempTablePrefix(propValue);
			}
			else if (key.equals("backuptableprefix"))
			{
				table.setBackupTablePrefix(propValue);
			}
			else if (key.equals("currenttableprefix"))
			{
				table.setCurrentTablePrefix(propValue);
			}
			else if (key.equals("usefastviewswitching"))
			{
				table.setUseFastViewSwitching(boolValue);
			}
			else if (key.equals("uselockedmode"))
			{
				table.setUseLockedMode(boolValue);
			}
			else if (key.equals("copymethod"))
			{
				propValue = propValue.toLowerCase();
				if (propValue.equals("copyinto"))
				{
					table.setCopyMethod(CopyTable.COPY_METHOD_COPYINTO);
				}
				else if (propValue.equals("copyintoviatempfile") || propValue.equals("copyintowithtempfile") || propValue.equals("copyintotempfile"))
				{
					table.setCopyMethod(CopyTable.COPY_METHOD_COPYINTO_VIA_TEMP_FILE);
				}
				else if (propValue.startsWith("insert"))
				{
					table.setCopyMethod(CopyTable.COPY_METHOD_INSERT);
				}
			}

			tablesToCopy.put(id, table);
		}

		// verify each specified has a from and to name and add temp tables
		// and add temptable configuration if copyViaTempTable
		Iterator<Entry<String, CopyTable>> iter = tablesToCopy.entrySet().iterator();
		ArrayList<String> missingResultTables = new ArrayList<String>(); 
		ArrayList<String> missingNames = new ArrayList<String>(); 
		while (iter.hasNext())
		{
			Entry<String, CopyTable> entry = iter.next();
			String id = entry.getKey();
			CopyTable table = entry.getValue();
			if (table.getCurrentTable() == null)
			{
				LOG.error("Configuration for '" + id + "' is missing a result table");
				missingResultTables.add(id);
				iter.remove();
				continue;
			}

			if (StringUtils.isEmpty(table.getFromName()))
			{
				LOG.error("Configuration for '" + id + "' is missing name of from table");
				missingNames.add(id);
				iter.remove();
				continue;
			}

			if (StringUtils.isEmpty(table.getCurrentTable().getNameWithPrefixes()))
			{
				LOG.warn("Configuration for '" + id
					+ "' is missing name of to table. Using name of from table ("
					+ table.getFromName() + ")");
				table.getCurrentTable().setName(table.getFromName());
			}
			
			// if no source database has been specified then the default source
			// is used
			if (StringUtils.isEmpty(table.getSource()))
			{
				// check if default source exists
				if (sourceDatabases.containsKey(DEFAULT_SOURCE_ID))
				{
					table.setSource(DEFAULT_SOURCE_ID);
					LOG.info("Using default source database for table with id '" + id + "'");
				}
				else
				{
					LOG.error("Table with id '" + id + "' has not specified a source database and no default source exists in configuration");
					iter.remove();
					continue;
				}
			}
			else
			{
				// check if source exists
				if (!sourceDatabases.containsKey(table.getSource()))
				{
					LOG.error("Table with id '" + id + "' has specified a source database (" + table.getSource() + ") "
							+ "which does not exist in configuration");
					iter.remove();
					continue;
				}
			}
				

			if (table.isCopyViaTempTable() && table.getTempTable() == null)
			{
				MonetDBTable tempTable = new MonetDBTable(table);
				tempTable.setTempTable(true);
				tempTable.setName(table.getToName());
				table.getMonetDBTables().add(tempTable);
			}
		}

		if(!missingResultTables.isEmpty())
		{
			EmailUtil.sendMail("Configuration is missing a result table: " + missingResultTables.toString(), "Configuration is missing a result table in monetdb", config);
		}
		
		if(!missingNames.isEmpty())
		{
			EmailUtil.sendMail("Configuration is missing name of from table : " + missingNames.toString(), "Configuration is missing name of from table in monetdb", config);
		}

		if (tablesToCopy.size() == 0)
		{
			LOG.error("Configuration has specified NO tables to copy!");
			EmailUtil.sendMail("Configuration has specified NO tables to copy!", "Configuration has specified NO tables to copy in monetdb", config);
		}
		else
		{
			LOG.info("The following tables will be copied: ");
			for (CopyTable table : tablesToCopy.values())
			{
				LOG.info("* " + table.getFromName() + " -> "
					+ table.getCurrentTable().getNameWithPrefixes());
			}
		}

		return tablesToCopy;
	}

	public Properties getDatabaseProperties()
	{
		return databaseProperties;
	}

	public void setDatabaseProperties(Properties databaseProperties)
	{
		this.databaseProperties = databaseProperties;
	}
	
	public String getSchedulerTable ()
	{
		return this.schedulerTable;
	}
	
	public String getSchedulerSource ()
	{
		return this.schedulerSource;
	}
	
	public String getSchedulerColumn ()
	{
		return this.schedulerColumn;
	}
	
	public boolean isSchedulingEnabled ()
	{
		return (!StringUtils.isEmpty(this.schedulerTable) && !StringUtils.isEmpty(this.schedulerColumn));
	}

	public File getConfigFile ()
	{
		return this.configFile;
	}
	
	public int getBatchSize()
	{
		return batchSize;
	}

	public void setBatchSize(int batchSize)
	{
		this.batchSize = batchSize;
	}

	public HashMap<String, CopyTable> getTablesToCopy()
	{
		return tablesToCopy;
	}
	
	public String getJobId ()
	{
		if (StringUtils.isEmpty(jobId))
		{		
			jobId = getConfigChecksum();
		}
				
		return "job-" + jobId;
	}
	
	public String getConfigChecksum ()
	{
		String checksum = null;
		
		try {
			checksum = sha1Checksum(this.configFile);
		} catch (NoSuchAlgorithmException | IOException e) {
			LOG.warn("Unable to calculate SHA-1 checksum of config file");
			
			// default to using file size
			checksum = String.valueOf(this.configFile.length());
		}
		
		return checksum;
	}
	
	public boolean allowMultipleInstances ()
	{
		return this.allowMultipleInstances;
	}
	
	public HashMap<String, SourceDatabase> getSourceDatabases ()
	{
		return this.sourceDatabases;
	}

}
