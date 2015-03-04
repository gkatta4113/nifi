package org.apache.nifi.pql.evaluation.order;

import java.util.Comparator;

public class CellValue<T> implements Comparable<CellValue<T>> {
	private final T value;
	private final int rowId;
	private final Comparator<T> valueComparator;
	
	public CellValue(final T value, final int rowId, final Comparator<T> valueComparator) {
		this.value = value;
		this.rowId = rowId;
		this.valueComparator = valueComparator;
	}

	public T getValue() {
		return value;
	}

	public int getRowId() {
		return rowId;
	}
	
	@Override
	public int compareTo(final CellValue<T> other) {
		if ( other == null ) {
			return 1;
		}
		
		if ( this == other ) {
			return 0;
		}
		
		return valueComparator.compare(value, other.value);
	}
	
	@Override
	public String toString() {
		return value.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		
		@SuppressWarnings("rawtypes")
		CellValue other = (CellValue) obj;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}
	
}
