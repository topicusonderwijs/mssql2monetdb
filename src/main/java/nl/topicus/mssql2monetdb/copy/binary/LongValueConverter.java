package nl.topicus.mssql2monetdb.copy.binary;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class LongValueConverter implements ValueConverter
{
	private ByteBuffer bb = ByteBuffer.allocate(8).order(ByteOrder.nativeOrder());

	@Override
	public byte[] convertValue(Object value) {
		bb.clear();
		if (value == null) {
			value = Long.MIN_VALUE;
		}
		
		if (value instanceof Long)
			bb.putLong((Long) value);
		else if (value instanceof BigDecimal)
			bb.putLong(((BigDecimal)value).longValue());
		else
			bb.putLong(Long.MIN_VALUE);
		
		return bb.array();
	}
}