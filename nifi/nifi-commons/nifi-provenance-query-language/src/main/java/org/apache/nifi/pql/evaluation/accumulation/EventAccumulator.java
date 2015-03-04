package org.apache.nifi.pql.evaluation.accumulation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.pql.evaluation.Accumulator;
import org.apache.nifi.pql.evaluation.OperandEvaluator;
import org.apache.nifi.pql.groups.Group;
import org.apache.nifi.provenance.ProvenanceEventRecord;

public class EventAccumulator implements Accumulator<Object>, Cloneable {

	private final long id;
	private final Map<Group, List<Object>> records = new LinkedHashMap<>();
	private final String label;
	private final OperandEvaluator<?> valueExtractor;
	private final boolean distinct;
	
	public EventAccumulator(final long id, final String label, final boolean distinct) {
		this(id, label, null, distinct);
	}
	
	public EventAccumulator(final long id, final String label, final OperandEvaluator<?> valueExtractor, final boolean distinct) {
		this.id = id;
		this.label = label;
		this.valueExtractor = valueExtractor;
		this.distinct = distinct;
	}
	
	@Override
	public Map<Group, List<Object>> getValues() {
		return Collections.unmodifiableMap(records);
	}

	@Override
	public List<Object> getValues(final Group group) {
		final List<Object> objects = records.get(group);
		if ( objects == null ) {
			return Collections.emptyList();
		}
		
		return Collections.unmodifiableList(objects);
	}
	
	@Override
	public Object accumulate(final ProvenanceEventRecord record, final Group group) {
		final Object value = valueExtractor == null ? record : valueExtractor.evaluate(record);
		
		if ( group == null ) {
		    return value;
		}
		
		List<Object> groupRecords = records.get(group);
		if ( groupRecords == null ) {
			groupRecords = new ArrayList<>();
			records.put(group, groupRecords);
		}
		
		if ( !distinct || !groupRecords.contains(value) ) {
			groupRecords.add( value );
		}
		
		return Collections.unmodifiableList(groupRecords);
	}

	@Override
	public String getLabel() {
		return label;
	}

	@Override
	public boolean isAggregateFunction() {
		return false;
	}

	@Override
	public Class<? extends Object> getReturnType() {
		return valueExtractor == null ? ProvenanceEventRecord.class : valueExtractor.getType();
	}

	@Override
	public EventAccumulator clone() {
		return new EventAccumulator(id, label, valueExtractor, distinct);
	}
	
	@Override
	public long getId() {
		return id;
	}

	@Override
	public void reset() {
		records.clear();
	}
}
