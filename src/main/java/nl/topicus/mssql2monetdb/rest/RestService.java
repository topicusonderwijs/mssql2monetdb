package nl.topicus.mssql2monetdb.rest;

import spark.Spark;

public class RestService {
	public static void main(String[] args) {       
		RestService service = new RestService();
		
		try {
			service.run();
		} catch (Exception e) {
			System.err.println("ERROR: " + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}	
	}
	
	public void run () throws Exception {
		// set port of REST service
		Spark.port(4567);		
		
		// index page
		Spark.get("/", (request, response) -> {
			return "<h1>MSSQL2MonetDB REST Service</h1>";
		});
		
		// setup status endpoint
		Spark.get("/status", new StatusRoute());
		Spark.post("/status", new StatusRoute());
		
		// setup run endpoint
		Spark.post("/run/:config", new RunRoute());
		Spark.get("/run/:config", (request, response) -> {
			return "<h1>This URI only accepts a POST request</h1>";
		});

	}
}
