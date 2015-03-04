package org.apache.nifi.pql.evaluation.order;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.pql.evaluation.Accumulator;
import org.apache.nifi.pql.groups.Group;
import org.apache.nifi.provenance.ProvenanceEventRecord;

public class GroupedSorter implements RowSorter {
	private final Map<Accumulator<?>, SortDirection> accumulators;
	private final Map<Group, Integer> firstGroupOccurrence = new HashMap<>();
	private final Comparator<Group> comparator;

	private final Set<CellValue<Group>> values = new HashSet<>();
	
	public GroupedSorter(final Map<Accumulator<?>, SortDirection> accumulators) {
		this.accumulators = accumulators;
		comparator = new GroupedComparator(accumulators);
	}

	@Override
	public void add(final ProvenanceEventRecord record, final Group group, final int rowId) {
		if ( !firstGroupOccurrence.containsKey(group) ) {
			firstGroupOccurrence.put(group, firstGroupOccurrence.size());
		}
		
		for ( final Accumulator<?> accum : accumulators.keySet() ) {
			accum.accumulate(record, group);
		}

		values.add(new CellValue<Group>(group, firstGroupOccurrence.get(group), comparator));
	}

	@Override
	public List<Integer> sort() {
		final List<CellValue<Group>> sortedGroups = new ArrayList<>();
		for ( final CellValue<Group> value : values ) {
			sortedGroups.add(value);
		}
		
		Collections.sort(sortedGroups);
		
		final List<Integer> sorted = new ArrayList<>(values.size());
		
		for ( final CellValue<Group> value : sortedGroups ) {
			sorted.add( value.getRowId() );
		}
		
		return sorted;
	}

	
	private static class GroupedComparator implements Comparator<Group> {
		private final Map<Accumulator<?>, SortDirection> map;
		
		public GroupedComparator(final Map<Accumulator<?>, SortDirection> map) {
			this.map = map;
		}
		
		@Override
		@SuppressWarnings({ "rawtypes", "unchecked" })
		public int compare(final Group r1, final Group r2) {
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
			if ( r1.equals(r2) ) {
				return 0;
			}
			
			for ( final Map.Entry<Accumulator<?>, SortDirection> entry : map.entrySet() ) {
				final Accumulator<?> accumulator = entry.getKey();
				final SortDirection dir = entry.getValue();
				
				final List<Object> rowValues1 = (List<Object>) accumulator.getValues(r1);
				final List<Object> rowValues2 = (List<Object>) accumulator.getValues(r2);
				
				if ( rowValues1.size() > rowValues2.size() ) {
					return -1;
				} else if ( rowValues2.size() > rowValues1.size() ) {
					return 1;
				}
				
				for (int i=0; i < rowValues1.size(); i++) {
					final Object v1 = rowValues1.get(i);
					final Object v2 = rowValues2.get(i);
					
					int comparisonResult;
					
					if ( Number.class.isAssignableFrom(v1.getClass()) ) {
						final Comparator comparator = Sorters.newNumberComparator();
						comparisonResult = comparator.compare((Number) v1, (Number) v2);
					} else {
						final Comparator comparator = Sorters.newObjectComparator();
						comparisonResult = comparator.compare(v1, v2);
					}
					
					if ( comparisonResult != 0 ) {
						return dir == SortDirection.ASC ? comparisonResult : -comparisonResult;
					}
				}
			}
			
			return 0;
		}
		
	}
}
