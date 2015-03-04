package org.apache.nifi.pql.results;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.nifi.pql.evaluation.Accumulator;
import org.apache.nifi.pql.evaluation.RecordEvaluator;
import org.apache.nifi.pql.evaluation.order.RowSorter;
import org.apache.nifi.pql.groups.Group;
import org.apache.nifi.pql.groups.Grouper;
import org.apache.nifi.provenance.StoredProvenanceEvent;
import org.apache.nifi.provenance.query.ProvenanceResultSet;

public class GroupingResultSet implements ProvenanceResultSet {
	private final List<String> labels;
	private final List<Class<?>> returnTypes;
	private final Iterator<? extends StoredProvenanceEvent> recordItr;
	private final List<Accumulator<?>> selectAccumulators;
	private final RecordEvaluator<Boolean> sourceEvaluator;
	private final RecordEvaluator<Boolean> conditionEvaluator;
	private final List<RecordEvaluator<?>> groupEvaluators;
	private final RowSorter sorter;
	private final Long limit;
	private long recordsReturned = 0L;
	
	private Iterator<List<Object>> rowItr;
	
	public GroupingResultSet(
			final Iterator<? extends StoredProvenanceEvent> recordItr,
			final List<Accumulator<?>> selectAccumulators,
			final RecordEvaluator<Boolean> sourceEvaluator, 
			final RecordEvaluator<Boolean> conditionEvaluator, 
			final List<String> labels, final List<Class<?>> returnTypes, final List<RecordEvaluator<?>> groupEvaluators, 
			final RowSorter sorter,
			final Long limit) {
		
		this.labels = labels;
		this.returnTypes = returnTypes;
		this.recordItr = recordItr;
		this.selectAccumulators = selectAccumulators;
		this.sourceEvaluator = sourceEvaluator;
		this.conditionEvaluator = conditionEvaluator;
		this.groupEvaluators = groupEvaluators;
		this.sorter = sorter;
		this.limit = limit;
	}
	

	@Override
	public List<String> getLabels() {
		return labels;
	}

	@Override
	public List<Class<?>> getReturnType() {
		return returnTypes;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void createRowItr() {
	    int recordIdx = 0;
        while (recordItr.hasNext()) {
            final StoredProvenanceEvent record = recordItr.next();
            
            if ( sourceEvaluator != null && !sourceEvaluator.evaluate(record) ) {
                continue;
            }
            
            final boolean meetsConditions = conditionEvaluator == null ? true : conditionEvaluator.evaluate(record);
            if ( meetsConditions ) {
                final Group group = groupEvaluators == null ? null : Grouper.group(record, groupEvaluators);
                
                for ( final Accumulator<?> accumulator : selectAccumulators ) {
                    accumulator.accumulate(record, group);
                }
                
                if ( sorter != null ) {
                    sorter.add(record, group, recordIdx++);
                }
            }
        }
        
        // Key = Group
        // Value = Map
        //          Key = Accumulator
        //          Value = Column values for a row
        final Map<Group, Map<Accumulator, List<Object>>> groupedMap = new LinkedHashMap<>();
        for ( final Accumulator<?> accumulator : selectAccumulators ) {
            final Map<Group, List<Object>> accumulatedValues = (Map) accumulator.getValues();
            
            // for each row returned by this accumulator...
            for ( final Map.Entry<Group, List<Object>> entry : accumulatedValues.entrySet() ) {
                final Group group = entry.getKey();
                
                Map<Accumulator, List<Object>> accumulatorRows = groupedMap.get(group);
                if ( accumulatorRows == null ) {
                    accumulatorRows = new LinkedHashMap<>();
                    groupedMap.put(group, accumulatorRows);
                }
                accumulatorRows.put(accumulator, accumulatedValues.get(group));
            }
        }
        
        final Collection<Map<Accumulator, List<Object>>> columnCollection = groupedMap.values();
        final List<List<Object>> rows = new ArrayList<>();
        for ( final Map<Accumulator, List<Object>> map : columnCollection ) {
            final List<Object> columnValues = new ArrayList<>();
            
            int rowIdx = 0;
            for ( final List<Object> accumulatorRows : map.values() ) {
                if (accumulatorRows.size() <= rowIdx) {
                    break;
                }
                
                final Object columnVal = accumulatorRows.get(rowIdx);
                columnValues.add(columnVal);
            }
            
            rowIdx++;
            rows.add(columnValues);
        }

        final List<List<Object>> sortedRows;
        if ( sorter == null ) {
            sortedRows = rows;
        } else {
            sortedRows = new ArrayList<>(rows.size());
            
            final List<Integer> sortedRowIds = sorter.sort();
            for (final Integer rowId : sortedRowIds) {
                sortedRows.add( rows.get(rowId) );
            }
        }
        
        rowItr = sortedRows.iterator();
	}
	
	
	@Override
	public boolean hasNext() {
	    if ( rowItr == null ) {
	        createRowItr();
	    }
		return (limit == null || recordsReturned <= limit ) && rowItr.hasNext();
	}

	@Override
	public List<?> next() {
	    if ( hasNext() ) {
	        return rowItr.next();
	    }
	    
	    throw new NoSuchElementException();
	}

}
