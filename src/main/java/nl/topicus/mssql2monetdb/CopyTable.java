package nl.topicus.mssql2monetdb;

import java.util.ArrayList;
import java.util.List;

/**
 * The global CopyTables object which contains the configuration for a table that needs to
 * be copied like which copy method should be used, but also contains {@link MonetDBTable}
 * s which is usually one, but can also contain a temporary table definition in case of
 * replaceTempTable.
 * 
 * @author bloemendal
 */
public class CopyTable
{
	public static final int COPY_METHOD_INSERT = 0;

	public static final int COPY_METHOD_COPYINTO = 1;

	// contains the actual result table and possible a temp table
	private List<MonetDBTable> monetDBTables = new ArrayList<MonetDBTable>();

	private boolean truncate = false;

	private boolean create = true;

	private boolean drop = false;

	private String fromName;

	private int copyMethod = COPY_METHOD_INSERT;

	// copies the table to a temp table and then replaces the 'to' table with the temp
	// table to reduce down-time
	private boolean copyViaTempTable = false;

	// prefix of the temp table that is created
	private String tempTablePrefix = "tmp_";

	// backup all tables, default is false
	private boolean backup = false;

	private String backupTablePrefix = "backup_";

	public void setCopyMethod(int copyMethod)
	{
		this.copyMethod = copyMethod;
	}

	public int getCopyMethod()
	{
		return this.copyMethod;
	}

	public void setDrop(boolean drop)
	{
		this.drop = drop;
	}

	public boolean drop()
	{
		return this.drop;
	}

	public void setTruncate(boolean truncate)
	{
		this.truncate = truncate;
	}

	public boolean truncate()
	{
		return this.truncate;
	}

	public void setCreate(boolean create)
	{
		this.create = create;
	}

	public boolean create()
	{
		return this.create;
	}

	public String getFromName()
	{
		return fromName;
	}

	public void setFromName(String fromName)
	{
		this.fromName = fromName;
	}

	public List<MonetDBTable> getMonetDBTables()
	{
		return monetDBTables;
	}

	public void setMonetDBTables(List<MonetDBTable> monetDBTables)
	{
		this.monetDBTables = monetDBTables;
	}

	public boolean isCopyViaTempTable()
	{
		return copyViaTempTable;
	}

	public void setCopyViaTempTable(boolean copyViaTempTable)
	{
		this.copyViaTempTable = copyViaTempTable;
	}

	public String getTempTablePrefix()
	{
		return tempTablePrefix;
	}

	public void setTempTablePrefix(String tempTablePrefix)
	{
		this.tempTablePrefix = tempTablePrefix;
	}

	public boolean isBackup()
	{
		return backup;
	}

	public void setBackup(boolean backup)
	{
		this.backup = backup;
	}

	public String getBackupTablePrefix()
	{
		return backupTablePrefix;
	}

	public void setBackupTablePrefix(String backupTablePrefix)
	{
		this.backupTablePrefix = backupTablePrefix;
	}

	public MonetDBTable getResultTable()
	{
		for (MonetDBTable table : monetDBTables)
			if (!table.isTempTable())
				return table;

		return null;
	}

	public MonetDBTable getTempTable()
	{
		for (MonetDBTable table : monetDBTables)
		{
			if (table.isTempTable())
				return table;
		}

		return null;
	}
}
