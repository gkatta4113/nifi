package org.apache.nifi.pql.results;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.nifi.pql.evaluation.Accumulator;
import org.apache.nifi.pql.evaluation.RecordEvaluator;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.StoredProvenanceEvent;
import org.apache.nifi.provenance.query.ProvenanceResultSet;

public class StandardUnorderedResultSet implements ProvenanceResultSet {

	private final List<String> labels;
	private final List<Class<?>> returnTypes;
	private final Iterator<? extends StoredProvenanceEvent> recordItr;
	private final RecordEvaluator<Boolean> sourceEvaluator;
	private final RecordEvaluator<Boolean> conditionEvaluator;
	private final List<Accumulator<?>> selectAccumulators;
	private final Long limit;
	
	private ResultRow nextRecord;
	private long recordsReturned = 0L;
	
	public StandardUnorderedResultSet(final Iterator<? extends StoredProvenanceEvent> recordItr,
			final List<Accumulator<?>> selectAccumulators,
			final RecordEvaluator<Boolean> sourceEvaluator, 
			final RecordEvaluator<Boolean> conditionEvaluator, 
			final List<String> labels, 
			final List<Class<?>> returnTypes,
			final Long limit)
	{
		this.selectAccumulators = selectAccumulators;
		this.labels = labels;
		this.returnTypes = returnTypes;
		this.recordItr = recordItr;
		this.sourceEvaluator = sourceEvaluator;
		this.conditionEvaluator = conditionEvaluator;
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

	private boolean findNextRecord() {
		if ( limit != null && recordsReturned >= limit.longValue() ) {
			return false;
		}
		
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
    			this.nextRecord = new ResultRow(values);
    			recordsReturned++;
    			return true;
    		}
    	}
		
		return false;
	}
	
	@Override
	public boolean hasNext() {
		if ( nextRecord != null ) {
			return true;
		}
		
		return findNextRecord();
	}

	@Override
	public List<?> next() {
		if ( hasNext() ) {
			final List<?> value = nextRecord.getValues();
			nextRecord = null;
			return value;
		}
		
		throw new NoSuchElementException();
	}

}
