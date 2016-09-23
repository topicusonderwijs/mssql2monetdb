package nl.topicus.mssql2monetdb.copy.binary;

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
		bb.putLong((Long) value);
		return bb.array();
	}
}