package nl.topicus.mssql2monetdb.copy.binary;

import java.math.BigDecimal;
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
		else if (value instanceof BigDecimal)
		{
			bb.putDouble(((BigDecimal)value).doubleValue());
		}
		else if (value instanceof Double)
		{
			bb.putDouble((Double) value);
		}
		else
		{
			bb.putDouble(Double.MIN_VALUE);
		}
		
		return bb.array();
	}
}