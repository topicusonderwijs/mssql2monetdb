package nl.topicus.mssql2monetdb.copy.binary;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class DoubleValueConverter implements ValueConverter
{
	private ByteBuffer bb = ByteBuffer.allocate(8).order(ByteOrder.nativeOrder());

	@Override
	public byte[] convertValue(Object value) {
		bb.clear();
		
		if (value == null) 
		{
			bb.putDouble(Double.MIN_VALUE);
		}		
		else 
		{
			bb.putDouble((Double) value);
		}
		
		return bb.array();
	}
}