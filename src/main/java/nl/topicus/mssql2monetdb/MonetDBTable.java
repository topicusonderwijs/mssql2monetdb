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

	private boolean tempTable = false;

	private boolean backupTable = false;

	private CopyTable copyTable;

	public MonetDBTable(CopyTable copyTable)
	{
		this.copyTable = copyTable;
	}

	/**
	 * Returns the name of the table with added prefixes if it's a temp or current table
	 * (for fast view switching).
	 */
	public String getNameWithPrefixes()
	{
		String ret;
		
		if (tempTable)
			ret = copyTable.getTempTablePrefix() + name;
		else if (copyTable.isUseFastViewSwitching())
			ret = name + "_" + copyTable.getLoadDate();
		else
			ret = name;
		
		return ret.toLowerCase();
	}

	public String getName()
	{
		return name;
	}

	public void setName(String name)
	{
		this.name = name.toLowerCase();
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

	public boolean isBackupTable()
	{
		return backupTable;
	}

	public void setBackupTable(boolean backupTable)
	{
		this.backupTable = backupTable;
	}

	public String getToTableSql()
	{
		String sql = "";

		if (StringUtils.isEmpty(copyTable.getSchema()) == false)
		{
			sql = MonetDBUtil.quoteMonetDbIdentifier(copyTable.getSchema());
			sql = sql + ".";
		}

		sql = sql + MonetDBUtil.quoteMonetDbIdentifier(getNameWithPrefixes());

		return sql;
	}

}
