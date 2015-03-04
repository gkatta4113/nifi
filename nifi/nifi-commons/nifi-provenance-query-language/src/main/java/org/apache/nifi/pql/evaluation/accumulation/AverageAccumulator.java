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

public class AverageAccumulator implements Accumulator<Double> {
	
	private final long id;
	private final RecordEvaluator<Long> evaluator;
	private final String label;
	private final Map<Group, Values> values = new LinkedHashMap<>();
	
	public AverageAccumulator(final long id, final RecordEvaluator<Long> extractor, final String label) {
		this.id = id;
		this.evaluator = extractor;
		this.label = label;
	}
	
	@Override
	public Map<Group, List<Double>> getValues() {
		final Map<Group, List<Double>> avgs = new HashMap<>(values.size());
		for ( final Map.Entry<Group, Values> entry : values.entrySet() ) {
			final Values values = entry.getValue();
			final double avg = (double) values.getSum() / (double) values.getCount();
			avgs.put(entry.getKey(), Collections.singletonList(avg));
		}
		return avgs;
	}

	@Override
	public List<Double> getValues(final Group group) {
		final Values v = values.get(group);
		if ( v == null ) {
			return Collections.emptyList();
		}
		
		final double d = v.getSum() / v.getCount();
		return Collections.singletonList(d);
	}
	
	public Double accumulate(final ProvenanceEventRecord record, final Group group) {
		final Long val = evaluator.evaluate(record);
		if ( val != null ) {
			Values v = values.get(group);
			if ( v == null ) {
				v = new Values();
				values.put(group, v);
			}
			
			v.increment(val.longValue());
			return (double) v.getSum() / (double) v.getCount();
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
	public Class<Double> getReturnType() {
		return Double.class;
	}
	
	@Override
	public AverageAccumulator clone() {
		return new AverageAccumulator(id, evaluator, label);
	}
	
	@Override
	public long getId() {
		return id;
	}

	@Override
	public void reset() {
		values.clear();
	}

	private static class Values {
		private long count;
		private long sum;
		
		public void increment(final long sum) {
			count++;
			this.sum += sum;
		}
		
		public long getCount() {
			return count;
		}
		
		public long getSum() {
			return sum;
		}
	}
}
