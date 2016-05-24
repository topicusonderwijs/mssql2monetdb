package nl.topicus.mssql2monetdb.rest;

import spark.Spark;

public class RestService {
	public static void main(String[] args) {       
		RestService service = new RestService();
		
		try {
			service.run(args);
		} catch (Exception e) {
			System.err.println("ERROR: " + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}	
	}
	
	public void run (String[] args) throws Exception {
		RestConfig conf = new RestConfig(args);
		
		// set port of REST service
		Spark.port(conf.getPort());	
		
		Spark.before(new AuthFilter(conf));
		
		// index page
		Spark.get("/", (request, response) -> {
			return "<h1>MSSQL2MonetDB REST Service</h1>";
		});
		
		// setup status endpoint
		Spark.get("/status", new StatusRoute());
		Spark.post("/status", new StatusRoute());
		
		// setup run endpoint
		Spark.post("/run/:config", new RunRoute(conf));
		Spark.get("/run/:config", (request, response) -> {
			return "<h1>This URI only accepts a POST request</h1>";
		});

	}
}
