package org.connid.bundles.db.table.security;

public class UnsupportedPasswordCharsetException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2764490740696573316L;

	public UnsupportedPasswordCharsetException() {
		super();
	}

	public UnsupportedPasswordCharsetException(String message, Throwable cause) {
		super(message, cause);
	}

	public UnsupportedPasswordCharsetException(String message) {
		super(message);
	}

	public UnsupportedPasswordCharsetException(Throwable cause) {
		super(cause);
	}

}
