package com.unesp.OpenMock.service;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

public final class BatchContracts {

	private BatchContracts() {
	}

	@JsonIgnoreProperties(ignoreUnknown = true)
	public record BatchResponse(List<CommentResponse> comments) {
	}

	@JsonIgnoreProperties(ignoreUnknown = true)
	public record CommentResponse(Long id) {
	}

	@JsonIgnoreProperties(ignoreUnknown = true)
	public record ClaimResponse(boolean claimed) {
	}

	public record BatchResultsRequest(List<CommentResult> comments, long tokensConsumed, String llmModel) {
	}

	public record CommentResult(Long id, String inferenceResult) {
	}

	public record FailureRequest(String reason) {
	}
}
