package nl.topicus.mssql2monetdb.rest;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nl.topicus.mssql2monetdb.tool.CopyTool;
import nl.topicus.mssql2monetdb.tool.CopyToolConfig;
import spark.Request;
import spark.Response;
import spark.Route;

public class RunRoute implements Route {
	private static final Logger LOG =  LoggerFactory.getLogger(RunRoute.class);

	private RestConfig restConf;
	
	public RunRoute(RestConfig restConf) {
		this.restConf = restConf;
	}
	
	private File findConfigFile (String configName)
	{
		File configFile = new File (restConf.getConfigPath(), configName + ".properties");
		
		if (!configFile.exists())
			configFile = new File (restConf.getConfigPath(), configName + ".props");
		
		if (!configFile.exists() || !configFile.isFile())
			return null;
		
		LOG.debug("Found config file '{}'", configFile.getAbsolutePath());
		
		return configFile;	
	}
	
	@Override
	public Object handle(Request request, Response response) throws Exception {
		String config = request.params("config");
		
		LOG.info("Started '{}' job by '{}'", config, request.ip());
		
		File configFile = findConfigFile(config);
		
		if (configFile == null)
		{
			LOG.warn("Job '{}' failed: unable to find config file", config);
			response.status(500);
			return "ERROR: unable to find config file";
		}
		
		String[] args = {"--config", configFile.getAbsolutePath()};
		
		try {
			CopyToolConfig toolConfig = new CopyToolConfig(args);
			
			CopyTool tool = new CopyTool();
			tool.run(toolConfig);
		} catch (Exception e) {
			LOG.warn("Job '{}' failed", config);
			response.status(500);
			return "ERROR: " + e.getMessage();
		}
		
		LOG.info("Job '{}' succeeded", config);
		return "OK";
	}

}
