# Use official Java base image
FROM java:8-jre
MAINTAINER Dennis Pallett, dennis.pallett@topicus.nl

# Make the log directory a volume so it's available to other containers
VOLUME ["/logs"]

# set entrypoint to shell script
ENTRYPOINT ["/docker-entrypoint.sh"]

# Add MSSQL2MonetDB jar file
ENV TOOL_VERSION 1.2
ADD https://github.com/topicus-ini/mssql2monetdb/releases/download/$TOOL_VERSION/mssql2monetdb-$TOOL_VERSION-jar-with-dependencies.jar /mssql2monetdb-$TOOL_VERSION.jar

# Copy logging config file
COPY log4j.properties /log4j.properties

# copy entrpoint Bash shell script
WORKDIR /
COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh