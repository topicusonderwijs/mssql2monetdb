package nl.topicus.mssql2monetdb;

import org.apache.commons.lang.StringUtils;

public class MonetDBTable
{
	private String fromName;

	private String toName;

	private String schema;

	private boolean tempTable = false;

	private CopyTable copyTable;

	public MonetDBTable(CopyTable copyTable)
	{
		this.copyTable = copyTable;
	}

	public String getFromName()
	{
		return fromName;
	}

	public void setFromName(String fromName)
	{
		this.fromName = fromName;
	}

	public String getToName()
	{
		return toName;
	}

	public void setToName(String toName)
	{
		this.toName = toName;
	}

	public String getSchema()
	{
		return schema;
	}

	public void setSchema(String schema)
	{
		this.schema = schema;
	}

	public boolean isTempTable()
	{
		return tempTable;
	}

	public void setTempTable(boolean tempTable)
	{
		this.tempTable = tempTable;
	}

	public CopyTable getCopyTable()
	{
		return copyTable;
	}

	public void setCopyTable(CopyTable copyTable)
	{
		this.copyTable = copyTable;
	}

	public String getToTableSql()
	{
		String sql = "";

		if (StringUtils.isEmpty(schema) == false)
		{
			sql = CopyTool.quoteMonetDbIdentifier(schema);
			sql = sql + ".";
		}

		sql = sql + CopyTool.quoteMonetDbIdentifier(toName);

		return sql;
	}

}
