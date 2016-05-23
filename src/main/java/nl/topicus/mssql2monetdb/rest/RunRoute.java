package nl.topicus.mssql2monetdb.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nl.topicus.mssql2monetdb.tool.CopyTool;
import nl.topicus.mssql2monetdb.tool.CopyToolConfig;
import spark.Request;
import spark.Response;
import spark.Route;

public class RunRoute implements Route {
	private static final Logger LOG =  LoggerFactory.getLogger(RunRoute.class);

	@Override
	public Object handle(Request request, Response response) throws Exception {
		String config = request.params("config");
		
		LOG.info("Started '{}' job by '{}'", config, request.ip());
		
		// TODO: find config file path
		
		
		
		String[] args = {"--config", "/Users/dennis/Downloads/test.props"};
		
		final CopyToolConfig toolConfig;
		
		try {
			toolConfig = new CopyToolConfig(args);
			
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
