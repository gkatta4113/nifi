package org.apache.nifi.pql.evaluation.order;

import java.util.List;

import org.apache.nifi.pql.groups.Group;
import org.apache.nifi.provenance.ProvenanceEventRecord;

public interface RowSorter {

	void add(ProvenanceEventRecord record, Group group, int rowId);
	
	List<Integer> sort();
}
