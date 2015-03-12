package org.apache.nifi.pql.groups;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Group {

	private final List<Object> values;
	private final int hashCode;
	
	public Group(final Object... values) {
		this(Arrays.asList(values));
	}
	
	public Group(final List<Object> values) {
		this.values = new ArrayList<>(values);
		
		int prime = 23497;
		int hc = 1;
		for ( final Object o : values ) {
			hc = prime * hc + (o == null ? 0 : o.hashCode());
		}
		
		this.hashCode = hc;
	}

	@Override
	public int hashCode() {
		return hashCode;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		
		Group other = (Group) obj;
		if (hashCode != other.hashCode)
			return false;
		if (values == null) {
			if (other.values != null)
				return false;
		} else if (!values.equals(other.values))
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		return values == null ? "Default Group" : values.toString();
	}
}
