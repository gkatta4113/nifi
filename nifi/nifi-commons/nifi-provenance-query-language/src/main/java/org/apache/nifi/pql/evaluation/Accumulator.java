package org.apache.nifi.pql.evaluation;

import java.util.List;
import java.util.Map;

import org.apache.nifi.pql.groups.Group;
import org.apache.nifi.provenance.ProvenanceEventRecord;

public interface Accumulator<T> extends Cloneable {

	T accumulate(ProvenanceEventRecord record, Group group);
	
	String getLabel();
	
	boolean isAggregateFunction();
	
	Class<? extends T> getReturnType();
	
	Accumulator<T> clone();
	
	long getId();
	
	void reset();
	
	Map<Group, List<T>> getValues();
	
	List<T> getValues(Group group);
}
