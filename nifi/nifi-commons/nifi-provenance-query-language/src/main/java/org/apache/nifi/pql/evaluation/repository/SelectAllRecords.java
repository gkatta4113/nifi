package org.apache.nifi.pql.evaluation.repository;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.nifi.pql.evaluation.RepositoryEvaluator;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.provenance.StoredProvenanceEvent;

public class SelectAllRecords implements RepositoryEvaluator {

	public Iterator<StoredProvenanceEvent> evaluate(final ProvenanceEventRepository repository) throws IOException {
		final int maxRecords = 10000;
		
		return new Iterator<StoredProvenanceEvent>() {
			long iterated = 0;
			long fetched = 0;
			
			List<StoredProvenanceEvent> records = null;
			Iterator<StoredProvenanceEvent> listItr = null;
			
			private void ensureIterator() {
				if ( listItr == null || !listItr.hasNext() ) {
					try {
						records = repository.getEvents(fetched, maxRecords);
					} catch (final IOException ioe) {
						throw new RuntimeException(ioe);
					}
					
					listItr = records.iterator();
					fetched += records.size();
				}
			}
			
			public boolean hasNext() {
				ensureIterator();
				return listItr.hasNext();
			}

			public StoredProvenanceEvent next() {
				if ( !hasNext() ) {
					throw new NoSuchElementException();
				}
				
				if ( iterated++ == fetched ) {
					records = null;
					listItr = null;
				}
				
				return listItr.next();
			}

			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}

}
