package org.apache.nifi.pql.evaluation.order;

import java.text.Collator;
import java.util.Comparator;

public class Sorters {

	public static Comparator<Number> newNumberComparator() {
		return new NumberComparator();
	}
	
	public static Comparator<Object> newObjectComparator() {
		return new ObjectComparator();
	}
	
	
	private static class NumberComparator implements Comparator<Number> {
		@Override
		public int compare(Number o1, Number o2) {
			if (o1 == o2) {
				return 0;
			}
			
			if (o1 == null && o2 == null) {
				return 0;
			}
			
			if (o1 == null) {
				return -1;
			}
			
			if (o2 == null) {
				return 1;
			}
			
			if (o1.doubleValue() < o2.doubleValue()) {
				return -1;
			}
			
			if (o1.doubleValue() > o2.doubleValue()) {
				return 1;
			}
			
			return 0;
		}
		
	}
	
	private static class ObjectComparator implements Comparator<Object> {
		private final Collator collator = Collator.getInstance();
		
		@Override
		public int compare(final Object o1, final Object o2) {
			if ( o1 == o2 ) {
				return 0;
			}
			
			if (o1 == null && o2 == null) {
				return 0;
			}
			
			if (o1 == null) {
				return -1;
			}
			
			if (o2 == null) {
				return 1;
			}
			
			return collator.compare(o1.toString(), o2.toString());
		}
	}
	
}
