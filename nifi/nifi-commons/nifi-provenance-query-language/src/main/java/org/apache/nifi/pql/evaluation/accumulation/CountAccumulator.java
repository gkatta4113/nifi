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

public class CountAccumulator implements Accumulator<Long> {

	private final long id;
	private final RecordEvaluator<?> evaluator;
	private final String label;
	private final Map<Group, Long> counts = new LinkedHashMap<>();
	
	
	public CountAccumulator(final long id, final RecordEvaluator<?> extractor, final String label) {
		this.id = id;
		this.evaluator = extractor;
		this.label = label;
	}
	
	@Override
	public Map<Group, List<Long>> getValues() {
		final Map<Group, List<Long>> map = new HashMap<>();
		for ( final Map.Entry<Group, Long> entry : counts.entrySet() ) {
			map.put(entry.getKey(), Collections.singletonList(entry.getValue()));
		}
		return map;
	}

	@Override
	public List<Long> getValues(final Group group) {
		final Long count = counts.get(group);
		if ( count == null ) {
			return Collections.emptyList();
		}
		
		return Collections.singletonList(count);
	}
	
	@Override
	public Long accumulate(final ProvenanceEventRecord record, final Group group) {
		final Object value = (evaluator == null) ? record : evaluator.evaluate(record);
		if ( value != null ) {
			Long val = counts.get(group);
			if ( val == null ) {
				val = 0L;
			}
			counts.put(group, val + 1);
			return val + 1;
		}
		
		return counts.get(group);
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
	public Class<? extends Long> getReturnType() {
		return Long.class;
	}

	@Override
	public long getId() {
		return id;
	}

	@Override
	public void reset() {
		counts.clear();
	}

	public CountAccumulator clone() {
		return new CountAccumulator(id, evaluator, label);
	}

}
