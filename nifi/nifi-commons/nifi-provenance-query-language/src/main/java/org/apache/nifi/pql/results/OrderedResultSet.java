package org.apache.nifi.pql.results;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.nifi.pql.evaluation.order.RowSorter;
import org.apache.nifi.provenance.query.ProvenanceResultSet;

public class OrderedResultSet implements ProvenanceResultSet {
	private final Iterator<List<?>> sortedRowItr;
	private final ProvenanceResultSet rs;
	
	public OrderedResultSet(final ProvenanceResultSet rs, final RowSorter sorter) {
		this.rs = rs;
		final List<List<?>> rows = new ArrayList<>();
		
		while (rs.hasNext()) {
			final List<?> colVals = rs.next();
			rows.add(colVals);
		}
		
		final List<List<?>> sortedRows = new ArrayList<>(rows.size());
		for ( final Integer rowId : sorter.sort() ) {
			sortedRows.add(rows.get(rowId.intValue()));
		}
		
		sortedRowItr = sortedRows.iterator();
	}
	
	@Override
	public List<String> getLabels() {
		return rs.getLabels();
	}
	
	@Override
	public List<Class<?>> getReturnType() {
		return rs.getReturnType();
	}
	
	@Override
	public boolean hasNext() {
		return sortedRowItr.hasNext();
	}
	
	@Override
	public List<?> next() {
		return sortedRowItr.next();
	}
}
