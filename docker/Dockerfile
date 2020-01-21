# Use official Java base image
FROM java:8-jre
MAINTAINER Dennis Pallett, dennis.pallett@topicus.nl

# Make the log directory a volume so it's available to other containers
VOLUME ["/logs"]

# set entrypoint to shell script
ENTRYPOINT ["/docker-entrypoint.sh"]

RUN mkdir /mssql2monetdb

# Add MSSQL2MonetDB jar file
COPY mssql2monetdb-*.jar /mssql2monetdb/mssql2monetdb.jar

ADD https://repo1.maven.org/maven2/log4j/log4j/1.2.17/log4j-1.2.17.jar /mssql2monetdb/
ADD https://repo1.maven.org/maven2/org/slf4j/slf4j-log4j12/1.7.21/slf4j-log4j12-1.7.21.jar /mssql2monetdb/

# Copy logging config file
COPY log4j.properties /mssql2monetdb/log4j.properties

# copy entrpoint Bash shell script
WORKDIR /
COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh