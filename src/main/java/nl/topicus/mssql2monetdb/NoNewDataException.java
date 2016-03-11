package nl.topicus.mssql2monetdb;

public class NoNewDataException extends Exception 
{
	private static final long serialVersionUID = 1L;

	public NoNewDataException (String msg)
	{
		super(msg);
	}	
}
