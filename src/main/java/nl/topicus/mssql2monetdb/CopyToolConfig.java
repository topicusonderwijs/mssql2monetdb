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

	private Properties databaseProperties;

	private int batchSize = DEFAULT_BATCH_SIZE;
	
	private String jobId;
	
	private String schedulerTable;
	
	private String schedulerColumn;
	
	private boolean allowMultipleInstances = false;
	
	private File configFile;

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
		this.tablesToCopy = findTablesToCopy(config);
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

			if (key.equals("from"))
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

}
