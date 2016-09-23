package nl.topicus.mssql2monetdb;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public abstract class AbstractCopyData 
{
	private CopyToolConfig config;
	
	public CopyToolConfig getConfig() {
		return config;
	}

	public void setConfig(CopyToolConfig config) {
		this.config = config;
	}
	
	public abstract long copyData (CopyTable copyTable, ResultSetMetaData metaData, ResultSet resultSet, long rowCount) throws SQLException, IOException;
}
