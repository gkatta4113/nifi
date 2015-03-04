package org.apache.nifi.pql.results;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RowIterator implements Iterator<ResultRow> {
	private final Iterator<Iterator<?>> itrs;
	
	public RowIterator(final List<Iterator<?>> itrs) {
		this.itrs = itrs.iterator();
	}
	
	
	@Override
	public boolean hasNext() {
		return itrs.hasNext();
	}
	
	public ResultRow next() {
		final Iterator<?> columnValueItr = itrs.next();
		final List<Object> colValues = new ArrayList<>();
		while (columnValueItr.hasNext()) {
			colValues.add(columnValueItr.next());
		}
		return new ResultRow(colValues);
	}
	
//	@Override
//	public boolean hasNext() {
//		if ( curItr == null || !curItr.hasNext() ) {
//			while (itrs.hasNext()) {
//				curItr = itrs.next();
//				if ( curItr.hasNext() ) {
//					return true;
//				}
//			}
//			
//			return false;
//		}
//		
//		return true;
//	}
//
//	@Override
//	public T next() {
//		if ( curItr.hasNext() ) {
//			return curItr.next();
//		}
//		
//		if ( curItr == null || !curItr.hasNext() ) {
//			while (itrs.hasNext()) {
//				curItr = itrs.next();
//				if ( curItr.hasNext() ) {
//					return curItr.next();
//				}
//			}
//			
//			throw new NoSuchElementException();
//		} else {
//			return curItr.next();
//		}
//	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
}
