package nl.topicus.mssql2monetdb.rest;

import spark.Request;
import spark.Response;
import spark.Route;

public class StatusRoute implements Route {

	@Override
	public Object handle(Request request, Response response) throws Exception {
		response.type("text/plain");
		response.status(200);
		return "OK";
	}

}
