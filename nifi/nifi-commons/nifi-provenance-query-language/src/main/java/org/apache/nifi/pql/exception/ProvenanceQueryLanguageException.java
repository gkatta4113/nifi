package org.apache.nifi.pql.exception;

public class ProvenanceQueryLanguageException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public ProvenanceQueryLanguageException() {
		super();
	}

	public ProvenanceQueryLanguageException(final String message) {
		super(message);
	}

	public ProvenanceQueryLanguageException(final Throwable cause) {
		super(cause);
	}

	public ProvenanceQueryLanguageException(final String message, final Throwable cause) {
		super(message, cause);
	}

}
