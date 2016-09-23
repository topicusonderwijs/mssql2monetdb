package nl.topicus.mssql2monetdb.copy.binary;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;

public class TimestampValueConverter implements ValueConverter
{
	/**
	 * Reference date used by MonetDB as starting point for dates
	 */
	private static final LocalDate REF_DATE = LocalDate.of(0, 1, 1);
	
	private ByteBuffer bb = ByteBuffer.allocate(8).order(ByteOrder.nativeOrder());
	
	@Override
	public byte[] convertValue(Object value) {
		bb.clear();
		
		if (value == null) {
			bb.putLong(Long.MIN_VALUE);
		} else {
			Timestamp ts = (Timestamp) value;
			Calendar cal = Calendar.getInstance();
			cal.setTimeInMillis(ts.getTime());

			// build up time value
			int time = cal.get(Calendar.HOUR_OF_DAY) * 3600;
			time += (cal.get(Calendar.MINUTE) * 60);
			time += cal.get(Calendar.SECOND);

			// convert into milliseconds
			time = time * 1000;
			
			// add nanoseconds part (in milliseconds, hence the division)
			time = time + (ts.getNanos() / 1000000);
			
			// build up date value
			LocalDate dateValue = LocalDate.of(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) + 1, cal.get(Calendar.DAY_OF_MONTH));					
			int date = (int)ChronoUnit.DAYS.between(REF_DATE, dateValue);
			
			// add to output
			bb.putInt(time);
			bb.putInt(date);
		}
		
		return bb.array();
	}
}