package org.apache.nifi.pql.evaluation.accumulation;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.pql.evaluation.Accumulator;
import org.apache.nifi.pql.evaluation.RecordEvaluator;
import org.apache.nifi.pql.groups.Group;
import org.apache.nifi.provenance.ProvenanceEventRecord;

public class SumAccumulator implements Accumulator<Long>, Cloneable {
	
	private final long id;
	private final RecordEvaluator<Long> evaluator;
	private final String label;
	private final Map<Group, Long> sums = new LinkedHashMap<>();
	
	public SumAccumulator(final long id, final RecordEvaluator<Long> extractor, final String label) {
		this.id = id;
		this.evaluator = extractor;
		this.label = label;
	}
	
	@Override
	public Map<Group, List<Long>> getValues() {
		final Map<Group, List<Long>> map = new HashMap<>();
		for ( final Map.Entry<Group, Long> entry : sums.entrySet() ) {
			map.put(entry.getKey(), Collections.singletonList(entry.getValue()));
		}
		return map;
	}
	
	@Override
	public List<Long> getValues(final Group group) {
		final Long sum = sums.get(group);
		if ( sum == null ) {
			return Collections.emptyList();
		}
		
		return Collections.singletonList(sum);
	}
	
	@Override
	public Long accumulate(final ProvenanceEventRecord record, final Group group) {
		final Long val = evaluator.evaluate(record);
		if ( val != null ) {
			Long curVal = sums.get(group);
			if ( curVal == null ) {
				curVal = 0L;
			}
			
			final long newVal = curVal + val;
			sums.put(group, newVal);
			return newVal;
		}
		
		return null;
	}

	@Override
	public String getLabel() {
		return label;
	}
	
	@Override
	public boolean isAggregateFunction() {
		return true;
	}
	
	@Override
	public Class<Long> getReturnType() {
		return Long.class;
	}
	
	public SumAccumulator clone() {
		return new SumAccumulator(id, evaluator, label);
	}
	
	@Override
	public long getId() {
		return id;
	}

	@Override
	public void reset() {
		sums.clear();
	}
}
