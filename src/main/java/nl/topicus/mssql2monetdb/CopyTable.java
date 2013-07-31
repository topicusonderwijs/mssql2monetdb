package nl.topicus.mssql2monetdb;

import org.apache.commons.lang.StringUtils;

public class CopyTable {
	private String fromName;
	
	private String toName;
	
	private String schema;
	
	private boolean truncate = false;
	
	private boolean create = true;
	
	private boolean drop = false;

	public void setFromName(String fromName) {
		this.fromName = fromName;
	}
	
	public String getFromName() {
		return this.fromName;
	}
	
	public void setToName (String toName) {
		this.toName = toName;
	}
	
	public String getToName () {
		return this.toName;
	}
	
	public void setSchema(String schema) {
		this.schema = schema;
	}
	
	public String getSchema () {
		return this.schema;
	}
	
	public void setDrop (boolean drop) {
		this.drop = drop;
	}
	
	public boolean drop () {
		return this.drop;
	}
	
	public void setTruncate (boolean truncate) {
		this.truncate = truncate;
	}
	
	public boolean truncate () {
		return this.truncate;
	}
	
	public void setCreate (boolean create) {
		this.create = create;
	}
	
	public boolean create () {
		return this.create;
	}
	
	public String getToTableSql () {
		String sql = "";
		
		if (StringUtils.isEmpty(schema) == false) {
			sql = CopyTool.quoteMonetDbIdentifier(schema);
			sql = sql + ".";
		}
		
		sql = sql + CopyTool.quoteMonetDbIdentifier(toName);
		
		return sql;
	}

}
