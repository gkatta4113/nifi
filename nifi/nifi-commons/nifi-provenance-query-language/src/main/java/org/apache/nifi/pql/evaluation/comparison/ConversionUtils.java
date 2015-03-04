package org.apache.nifi.pql.evaluation.comparison;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConversionUtils {

    public static final String DATE_TO_STRING_FORMAT = "EEE MMM dd HH:mm:ss zzz yyyy";
    public static final Pattern DATE_TO_STRING_PATTERN = Pattern.compile("(?:[a-zA-Z]{3} ){2}\\d{2} \\d{2}\\:\\d{2}\\:\\d{2} (?:.*?) \\d{4}");
    
    public static final String ALTERNATE_FORMAT_WITHOUT_MILLIS = "yyyy/MM/dd HH:mm:ss";
    public static final String ALTERNATE_FORMAT_WITH_MILLIS = "yyyy/MM/dd HH:mm:ss.SSS";
    public static final Pattern ALTERNATE_PATTERN = Pattern.compile("\\d{4}/\\d{2}/\\d{2} \\d{2}\\:\\d{2}\\:\\d{2}(\\.\\d{3})?");

    public static final Pattern NUMBER_PATTERN = Pattern.compile("-?\\d+");

    
    public static Long convertToLong(final Object o) {
    	if ( o == null ) {
    		return null;
    	}
    	
    	if (o instanceof Long) {
    		return (Long) o;
    	}
    	
    	if (o instanceof Number) {
    		return ((Number) o).longValue();
    	}
    	
    	if ( o instanceof Date ) {
    		return ((Date) o).getTime();
    	}
    	
    	if ( o instanceof Calendar ) {
    		return ((Calendar) o).getTimeInMillis();
    	}
    	
    	if ( o instanceof String ) {
    		return convertToLong((String) o);
    	}
    	
    	return null;
    }
    
	public static Long convertToLong(final String value) {
		if ( value == null ) {
			return null;
		}
		
		final String trimmed = value.trim();
		if ( trimmed.isEmpty() ) {
			return null;
		}
		
		
		if ( DATE_TO_STRING_PATTERN.matcher(trimmed).matches() ) {
            final SimpleDateFormat sdf = new SimpleDateFormat(DATE_TO_STRING_FORMAT);
            
            try {
                final Date date = sdf.parse(trimmed);
                return date.getTime();
            } catch (final ParseException pe) {
                return null;
            }
        } else if ( NUMBER_PATTERN.matcher(trimmed).matches() ) {
            return Long.valueOf(trimmed);
        } else {
            final Matcher altMatcher = ALTERNATE_PATTERN.matcher(trimmed);
            if ( altMatcher.matches() ) {
                final String millisValue = altMatcher.group(1);
                
                final String format;
                if ( millisValue == null ) {
                    format = ALTERNATE_FORMAT_WITHOUT_MILLIS;
                } else {
                    format = ALTERNATE_FORMAT_WITH_MILLIS;
                }
                
                final SimpleDateFormat sdf = new SimpleDateFormat(format);
                
                try {
                    return sdf.parse(trimmed).getTime();
                } catch (final ParseException pe) {
                    return null;
                }
            } else {
                return null;
            }
        }
	}
	
}
