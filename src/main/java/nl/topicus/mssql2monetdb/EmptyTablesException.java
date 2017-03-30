package nl.topicus.mssql2monetdb;

public class EmptyTablesException extends Exception
{
	private static final long serialVersionUID = 1L;

	public EmptyTablesException(String msg) {
		super(msg);
	}
}
