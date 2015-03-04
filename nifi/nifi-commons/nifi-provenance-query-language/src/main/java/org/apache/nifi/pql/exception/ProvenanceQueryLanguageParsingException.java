package org.apache.nifi.pql.exception;

public class ProvenanceQueryLanguageParsingException extends ProvenanceQueryLanguageException {

	private static final long serialVersionUID = 1L;

	public ProvenanceQueryLanguageParsingException() {
		super();
	}

	public ProvenanceQueryLanguageParsingException(final String message) {
		super(message);
	}

	public ProvenanceQueryLanguageParsingException(final Throwable cause) {
		super(cause);
	}

	public ProvenanceQueryLanguageParsingException(final String message, final Throwable cause) {
		super(message, cause);
	}
	
}
