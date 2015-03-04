package org.apache.nifi.pql.results;

import java.util.Arrays;
import java.util.List;

public class ResultRow {
	private final List<Object> values;
	
	public ResultRow(final Object... values) {
		this(Arrays.asList(values));
	}
	
	public ResultRow(final List<Object> values) {
		this.values = values;
	}
	
	public List<Object> getValues() {
		return values;
	}
}
