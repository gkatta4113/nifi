package org.apache.nifi.pql.results;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.nifi.pql.evaluation.Accumulator;
import org.apache.nifi.pql.evaluation.RecordEvaluator;
import org.apache.nifi.pql.evaluation.order.RowSorter;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.StoredProvenanceEvent;
import org.apache.nifi.provenance.query.ProvenanceResultSet;

public class StandardOrderedResultSet implements ProvenanceResultSet {
	private final List<String> labels;
	private final List<Class<?>> returnTypes;
	
	private final Iterator<? extends StoredProvenanceEvent> recordItr;
	private final List<Accumulator<?>> selectAccumulators;
	private final RecordEvaluator<Boolean> sourceEvaluator;
	private final RecordEvaluator<Boolean> conditionEvaluator;
	private final RowSorter sorter;
	private final Long limit;
	
	private Iterator<ResultRow> resultRowItr;

	public StandardOrderedResultSet(final Iterator<? extends StoredProvenanceEvent> recordItr,
			final List<Accumulator<?>> selectAccumulators,
			final RecordEvaluator<Boolean> sourceEvaluator, 
			final RecordEvaluator<Boolean> conditionEvaluator, 
			final List<String> labels, 
			final List<Class<?>> returnTypes,
			final RowSorter sorter,
			final Long limit)
	{
		this.labels = labels;
		this.returnTypes = returnTypes;
		
		this.recordItr = recordItr;
		this.selectAccumulators = selectAccumulators;
		this.sourceEvaluator = sourceEvaluator;
		this.conditionEvaluator = conditionEvaluator;
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

	private void createResultRowItr() {
	    final List<ResultRow> rows = new ArrayList<>();
        int idx = 0;
        while (recordItr.hasNext()) {
            final ProvenanceEventRecord record = recordItr.next();
            if ( sourceEvaluator != null && !sourceEvaluator.evaluate(record) ) {
                continue;
            }
            
            final boolean meetsConditions = conditionEvaluator == null ? true : conditionEvaluator.evaluate(record);
            if ( meetsConditions ) {
                final List<Object> values = new ArrayList<>(selectAccumulators.size());
                for ( final Accumulator<?> accumulator : selectAccumulators ) {
                    final Object value = accumulator.accumulate(record, null);
                    accumulator.reset();
                    values.add(value);
                }
                rows.add(new ResultRow(values));
                sorter.add(record, null, idx++);
            }
        }
        
        final List<ResultRow> sortedRows = new ArrayList<>();
        for ( final Integer unsortedIndex : sorter.sort() ) {
            final ResultRow row = rows.get(unsortedIndex.intValue());
            sortedRows.add(row);
            
            if ( limit != null && sortedRows.size() >= limit.intValue() ) {
                break;
            }
        }
        
        resultRowItr = sortedRows.iterator();
	}
	
	@Override
	public boolean hasNext() {
	    if ( resultRowItr == null ) {
	        createResultRowItr();
	    }
	    
		return resultRowItr.hasNext();
	}

	@Override
	public List<?> next() {
	    if ( resultRowItr == null ) {
            createResultRowItr();
        }
	    
		return resultRowItr.next().getValues();
	}

}
