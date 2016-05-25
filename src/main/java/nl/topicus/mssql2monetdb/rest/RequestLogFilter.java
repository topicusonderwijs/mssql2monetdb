package nl.topicus.mssql2monetdb.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import spark.Filter;
import spark.Request;
import spark.Response;

public class RequestLogFilter implements Filter {
	private static final Logger LOG =  LoggerFactory.getLogger(RequestLogFilter.class);
	
	@Override
	public void handle(Request request, Response response) throws Exception {
		LOG.info("{host: \"{}\", path: \"{}\", method: \"{}\", ipAddress: \"{}\", userAgent: \"{}\"}", request.host(), request.pathInfo(), request.requestMethod(), request.ip(), request.userAgent());	
	}

}
