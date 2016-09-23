package nl.topicus.mssql2monetdb.load;

import nl.topicus.mssql2monetdb.CopyTable;
import nl.topicus.mssql2monetdb.CopyToolConfig;
import nl.topicus.mssql2monetdb.MonetDBTable;
import nl.topicus.mssql2monetdb.util.SerializableResultSetMetaData;

public abstract class AbstractLoadData 
{
	private CopyToolConfig config;
	
	public CopyToolConfig getConfig() {
		return config;
	}

	public void setConfig(CopyToolConfig config) {
		this.config = config;
	}
	
	public abstract void loadData (SerializableResultSetMetaData metaData, CopyTable table, MonetDBTable copyToTable, Long insertCount) throws Exception;


}
