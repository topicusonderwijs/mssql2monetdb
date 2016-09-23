package nl.topicus.mssql2monetdb.copy.binary;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class StringValueConverter implements ValueConverter
{
	@Override
	public byte[] convertValue(Object value) {
		String out = (String)value;
		
		if (out != null)
			out = out.replace("\n", "\\\\n");
				
		return (out + "\n").getBytes();
	}
}