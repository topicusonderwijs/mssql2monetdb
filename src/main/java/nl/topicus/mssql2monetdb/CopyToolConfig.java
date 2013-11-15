package nl.topicus.mssql2monetdb;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;

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

	private HashMap<String, CopyTable> tablesToCopy = new HashMap<String, CopyTable>();

	public CopyToolConfig(String args[])
	{
		PropertyConfigurator.configure("log4j.properties");

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

		File configFile = new File(cmd.getOptionValue("config"));
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

		for (CONFIG_KEYS key : CONFIG_KEYS.values())
		{
			String value = config.getProperty(key.toString());
			if (key.isRequired() && StringUtils.isEmpty(value))
			{
				isMissing = true;
				LOG.error("Missing config property: " + key);
			}
		}

		if (isMissing)
		{
			LOG.fatal("Missing essential config properties");
			System.exit(1);
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
				// set current table because the toName will be used for the view
				table.getCurrentTable().setName("current_" + propValue.toLowerCase());
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
			else if (key.equals("backup"))
			{
				table.setBackup(boolValue);
			}
			else if (key.equals("backuptableprefix"))
			{
				table.setBackupTablePrefix(propValue);
			}

			tablesToCopy.put(id, table);
		}

		// verify each specified has a from and to name and add temp tables
		// and add temptable configuration if copyViaTempTable
		Iterator<Entry<String, CopyTable>> iter = tablesToCopy.entrySet().iterator();
		while (iter.hasNext())
		{
			Entry<String, CopyTable> entry = iter.next();
			String id = entry.getKey();
			CopyTable table = entry.getValue();
			if (table.getCurrentTable() == null)
			{
				LOG.error("Configuration for '" + id + "' is missing a result table");
				iter.remove();
				continue;
			}

			if (StringUtils.isEmpty(table.getFromName()))
			{
				LOG.error("Configuration for '" + id + "' is missing name of from table");
				iter.remove();
				continue;
			}

			if (StringUtils.isEmpty(table.getCurrentTable().getName()))
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
				// toName = tempTablePrefix + toName
				tempTable.setName(table.getTempTablePrefix() + table.getToName());
				table.getMonetDBTables().add(tempTable);
			}
		}

		if (tablesToCopy.size() == 0)
		{
			LOG.error("Configuration has specified NO tables to copy!");
		}
		else
		{
			LOG.info("The following tables will be copied: ");
			for (CopyTable table : tablesToCopy.values())
			{
				LOG.info("* " + table.getFromName() + " -> " + table.getCurrentTable().getName());
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

}
