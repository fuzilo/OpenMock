package com.unesp.OpenMock.service;

public class InferenceUnavailableException extends RuntimeException {

	public InferenceUnavailableException(String message) {
		super(message);
	}

	public InferenceUnavailableException(String message, Throwable cause) {
		super(message, cause);
	}
}
