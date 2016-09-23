package nl.topicus.mssql2monetdb.copy.binary;

public interface ValueConverter 
{
	public byte[] convertValue(Object value);
}