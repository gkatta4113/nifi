package org.apache.nifi.provenance.query;

import java.util.List;

import org.apache.nifi.provenance.ProvenanceEventRepository;


/**
 * Represents a set of results from issuing a query against a {@link ProvenanceEventRepository}.
 */
public interface ProvenanceResultSet {
    /**
     * Returns the labels for the columns (aka column headers)
     * @return
     */
	List<String> getLabels();
	
	/**
	 * Returns the types of the columns returned for each row
	 * @return
	 */
	List<Class<?>> getReturnType();
	
	/**
	 * Indicates whether or not another result exists in the result set.
	 * @return
	 */
	boolean hasNext();
	
	/**
	 * Returns the next result for this query
	 * @return
	 */
	List<?> next();
	
}
