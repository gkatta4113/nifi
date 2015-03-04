package org.apache.nifi.pql.evaluation.order;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.nifi.pql.evaluation.OperandEvaluator;
import org.apache.nifi.pql.groups.Group;
import org.apache.nifi.provenance.ProvenanceEventRecord;

public class FieldSorter implements RowSorter {

	private final SortedSet<CellValue<ProvenanceEventRecord>> values = new TreeSet<>();
	private final MultiFieldComparator comparator;
	
	public FieldSorter(final Map<OperandEvaluator<?>, SortDirection> fieldEvaluators) {
		comparator = new MultiFieldComparator(fieldEvaluators);
	}


	@Override
	public void add(final ProvenanceEventRecord record, final Group group, final int rowId) {
		values.add(new CellValue<ProvenanceEventRecord>(record, rowId, comparator));
	}

	@Override
	public List<Integer> sort() {
		final List<Integer> rowIds = new ArrayList<>();
		for ( final CellValue<?> value : values ) {
			rowIds.add( value.getRowId() );
		}
		return rowIds;
	}
	

	
	private static class MultiFieldComparator implements Comparator<ProvenanceEventRecord> {
		private final Map<OperandEvaluator<?>, SortDirection> evals;
		private final Comparator<Number> numberComparator = Sorters.newNumberComparator();
		private final Comparator<Object> objectComparator = Sorters.newObjectComparator();
		
		public MultiFieldComparator(final Map<OperandEvaluator<?>, SortDirection> evals) {
			this.evals = evals;
		}
		
		@Override
		public int compare(final ProvenanceEventRecord r1, final ProvenanceEventRecord r2) {
			if ( r1 == r2 ) {
				return 0;
			}
			if (r1 == null && r2 == null) {
				return 0;
			}
			if (r1 == null) {
				return -1;
			}
			if (r2 == null) {
				return 1;
			}
			
			for ( final Map.Entry<OperandEvaluator<?>, SortDirection> entry : evals.entrySet() ) {
				final OperandEvaluator<?> eval = entry.getKey();
				final SortDirection dir = entry.getValue();
				
				int comparisonResult;
				
				final Object v1 = eval.evaluate(r1);
				final Object v2 = eval.evaluate(r2);
				
				if ( Number.class.isAssignableFrom(eval.getType()) ) {
					comparisonResult = numberComparator.compare((Number) v1, (Number) v2);
				} else {
					comparisonResult = objectComparator.compare(v1, v2);
				}
				
				if ( comparisonResult != 0 ) {
					return dir == SortDirection.ASC ? comparisonResult : -comparisonResult;
				}
			}
			
			return 0;
		}
	}
}
