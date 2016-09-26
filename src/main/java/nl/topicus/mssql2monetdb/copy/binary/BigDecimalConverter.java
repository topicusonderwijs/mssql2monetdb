package nl.topicus.mssql2monetdb.copy.binary;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class BigDecimalConverter implements ValueConverter
{
	private BigDecimal precisionMultiplier;
	
	private ByteBuffer bb = ByteBuffer.allocate(8).order(ByteOrder.nativeOrder());
	
	public BigDecimalConverter (int precision)
	{
		precisionMultiplier = new BigDecimal(Math.pow(10, precision));
	}
	
	@Override
	public byte[] convertValue(Object value) {
		bb.clear();
		
		if (value == null)
		{
			bb.putLong(Long.MIN_VALUE);
		}
		else
		{
			BigDecimal decimal = (BigDecimal)value;
			decimal = decimal.multiply(precisionMultiplier);
			bb.putLong(decimal.longValue());
		}
		
		return bb.array();
	}
}
