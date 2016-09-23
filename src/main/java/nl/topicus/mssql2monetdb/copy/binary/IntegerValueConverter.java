package nl.topicus.mssql2monetdb.copy.binary;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class IntegerValueConverter implements ValueConverter
{
	private ByteBuffer bb = ByteBuffer.allocate(4).order(ByteOrder.nativeOrder());

	@Override
	public byte[] convertValue(Object value) {
		bb.clear();
		if (value == null) {
			value = Integer.MIN_VALUE;
		}
		bb.putInt((Integer) value);
		return bb.array();
	}
}