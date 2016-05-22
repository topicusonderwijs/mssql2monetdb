package nl.topicus.mssql2monetdb.rest;

import spark.Request;
import spark.Response;
import spark.Route;

public class RunRoute implements Route {

	@Override
	public Object handle(Request request, Response response) throws Exception {
		String config = request.params("config");
		return "TODO: run " + config;
	}

}
