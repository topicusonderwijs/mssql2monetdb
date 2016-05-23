package nl.topicus.mssql2monetdb.rest;

import nl.topicus.mssql2monetdb.tool.ConfigException;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestConfig {
	private static final Logger LOG =  LoggerFactory.getLogger(RestConfig.class);
	
	private CommandLine cmd;
	
	private String configPath;
	
	private int port;
	
	public RestConfig (String[] args) throws ConfigException
	{
		final Options options = new Options();
		
		OptionBuilder.hasArg(true);
		OptionBuilder.isRequired(false);
		OptionBuilder.withDescription("Specify the path of the configuration properties files");
		OptionBuilder.withLongOpt("configpath");
		options.addOption(OptionBuilder.create("c"));
		
		OptionBuilder.hasArg(true);
		OptionBuilder.isRequired(false);
		OptionBuilder.withDescription("Specify the port of the REST service");
		OptionBuilder.withLongOpt("port");
		options.addOption(OptionBuilder.create("P"));
		
		CommandLineParser parser = new BasicParser();
		try
		{
			cmd = parser.parse(options, args);
		}
		catch (ParseException e)
		{
			LOG.error("ERROR: " + e.getMessage());

			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("mssql2monetdb-rest", options);

			throw new ConfigException(e.getMessage());
		}
		if (cmd == null)
		{
			LOG.error("CommandLine parser is null");
			return;
		}
		
		initConfig();
	}
	
	private void initConfig () {
		this.port = getConfigValueAsInt("port", 4567);
		this.configPath = getConfigValue("configpath");
	}
	
	private int getConfigValueAsInt (String prop, int defaultValue)
	{
		String value = getConfigValue(prop);
		
		if (value == null)
			return defaultValue;
		
		try {
			return Integer.parseInt(value);
		} catch (NumberFormatException e) {
			LOG.warn("Invalid value '{}' specified for '{}', using default value '{}'", value, prop, defaultValue);
			return defaultValue;
		}
	}
	
	private String getConfigValue (String prop)
	{
		if (cmd.hasOption(prop))
			return cmd.getOptionValue(prop);
		
		String value = System.getProperty(prop);
		
		if (value != null)
			return value;
		
		value = System.getenv("MSSQL2MONETDB_REST_" + prop.toUpperCase());
		return value;
	}

	public String getConfigPath() {
		return configPath;
	}

	public void setConfigPath(String configPath) {
		this.configPath = configPath;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}
}
