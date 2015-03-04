package org.apache.nifi.pql.evaluation;

import java.io.IOException;
import java.util.Iterator;

import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.provenance.StoredProvenanceEvent;

public interface RepositoryEvaluator {

	Iterator<StoredProvenanceEvent> evaluate(ProvenanceEventRepository repository) throws IOException;
	
}
