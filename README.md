#MS SQL to MonetDB
###a table copy tool for MS SQL to MonetDB

A tool to copy a table or multiple tables from a MS SQL database to a MonetDB adatabase

### How to build and run.

Clean and make a Eclipse project.
`$ mvn clean eclipse:eclipse -U`

Make a single jar with embedded dependencies.
`$ mvn clean compile assembly:single`

Run.
`$ java -jar target/mssql2monetdb-0.1-SNAPSHOT-jar-with-dependencies.jar -c config.properties`

See the sample.config.properties file for an example of how the config file should like.