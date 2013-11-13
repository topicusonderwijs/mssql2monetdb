package nl.topicus.mssql2monetdb;

import nl.topicus.mssql2monetdb.util.MonetDBUtil;

import org.apache.commons.lang.StringUtils;

/**
 * Representation of a MonetDB table.
 * 
 * @author bloemendal
 */
public class MonetDBTable
{
	private String name;

	private String schema;

	private boolean tempTable = false;

	private CopyTable copyTable;

	public MonetDBTable(CopyTable copyTable)
	{
		this.copyTable = copyTable;
	}

	public String getName()
	{
		return name;
	}

	public void setName(String name)
	{
		this.name = name;
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
			sql = MonetDBUtil.quoteMonetDbIdentifier(schema);
			sql = sql + ".";
		}

		sql = sql + MonetDBUtil.quoteMonetDbIdentifier(name);

		return sql;
	}

}
