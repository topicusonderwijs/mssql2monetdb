package nl.topicus.mssql2monetdb.rest;

import java.util.Base64;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import spark.Filter;
import spark.Request;
import spark.Response;
import spark.Spark;

public class AuthFilter implements Filter {
	private static final Logger LOG =  LoggerFactory.getLogger(AuthFilter.class);
	
	private RestConfig restConf;
	
	public AuthFilter (RestConfig restConf) {
		this.restConf = restConf;
		
		if (isAuthEnabled()) {
			LOG.info("Enabled authentication");
		}
	}

	public boolean isAuthEnabled () {
		return StringUtils.isNotEmpty(restConf.getUser()) || StringUtils.isNotEmpty(restConf.getPassword());
	}
	
	@Override
	public void handle(Request request, Response response) throws Exception {
		if (!isAuthEnabled())
			return;
		
		if (!isAuthenticated (request)) {
			LOG.info("Request not authenticated");
			response.header("WWW-Authenticate", "Basic realm=\"This REST service requires a username and/or password\"");
			Spark.halt(401, "This REST service requires a username and password.");	
		}
	}
	
	private boolean isAuthenticated (Request request) {
		String auth = request.headers("Authorization");
		
		if (StringUtils.isEmpty(auth))
			return false;
		
		LOG.debug("Received authorization header: {}", auth);
		
		String[] split = auth.split(" ");
		
		if (split.length < 2)
			return false;
		
		// only accept basic authentication
		if (!split[0].equalsIgnoreCase("basic"))
			return false;
		
		// username and password to compare against, Base64 encoded
		String userPass = String.format("%s:%s", 
				StringUtils.isNotEmpty(restConf.getUser()) ? restConf.getUser() : "",
				StringUtils.isNotEmpty(restConf.getPassword()) ? restConf.getPassword() : ""
		);
		String compareStr = Base64.getEncoder().encodeToString(userPass.getBytes());
		
		// return result of comparison
		return compareStr.equals(split[1]);
	}

}
