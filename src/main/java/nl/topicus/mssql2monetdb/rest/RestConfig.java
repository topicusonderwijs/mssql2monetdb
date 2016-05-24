package nl.topicus.mssql2monetdb.rest;

import java.io.File;

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
	
	private File configPath;
	
	private int port;
	
	private String user;
	
	private String password;
	
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
		
		OptionBuilder.hasArg(true);
		OptionBuilder.isRequired(false);
		OptionBuilder.withDescription("Specify an optional user for the REST service");
		OptionBuilder.withLongOpt("user");
		options.addOption(OptionBuilder.create("u"));
		
		OptionBuilder.hasArg(true);
		OptionBuilder.isRequired(false);
		OptionBuilder.withDescription("Specify an optional password for the REST service");
		OptionBuilder.withLongOpt("password");
		options.addOption(OptionBuilder.create("up"));
		
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
	
	private void initConfig () throws ConfigException {
		this.port = getConfigValueAsInt("port", 4567);
		this.user = getConfigValue("user", false);
		this.password = getConfigValue("password", false);
		
		initConfigPath();
	}
	
	private void initConfigPath () throws ConfigException {
		setConfigPath(getConfigValue("configpath", true));
		
		if (!configPath.exists())
			throw new ConfigException("Configpath '%s' does not exist", configPath.getAbsolutePath());
		
		if (!configPath.isDirectory())
			throw new ConfigException("Configpath '%s' is not a directory", configPath.getAbsolutePath());
	}
	
	private int getConfigValueAsInt (String prop, int defaultValue) throws ConfigException
	{
		String value = getConfigValue(prop, false);
		
		if (value == null)
			return defaultValue;
		
		try {
			return Integer.parseInt(value);
		} catch (NumberFormatException e) {
			LOG.warn("Invalid value '{}' specified for '{}', using default value '{}'", value, prop, defaultValue);
			return defaultValue;
		}
	}
	
	private String getConfigValue (String prop, boolean isRequired) throws ConfigException
	{
		if (cmd.hasOption(prop))
			return cmd.getOptionValue(prop);
		
		String value = System.getProperty(prop);
		
		if (value != null)
			return value;
		
		value = System.getenv("MSSQL2MONETDB_REST_" + prop.toUpperCase());
		
		if (value == null && isRequired) {
			throw new ConfigException("Missing configuration option '" + prop + "'");
		}
		
		return value;
	}

	public File getConfigPath() {
		return configPath;
	}

	public void setConfigPath(String configPathStr) {
		this.configPath = new File(configPathStr);
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}
}
