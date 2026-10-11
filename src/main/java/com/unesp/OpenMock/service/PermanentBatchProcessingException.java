package com.unesp.OpenMock.service;

public class PermanentBatchProcessingException extends RuntimeException {

	public PermanentBatchProcessingException(String message) {
		super(message);
	}

	public PermanentBatchProcessingException(String message, Throwable cause) {
		super(message, cause);
	}
}
