package com.unesp.OpenMock.service;

import static com.unesp.OpenMock.service.BatchContracts.*;

import java.util.UUID;

import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;

@Component
public class SocialGuardClient {

	private final RestClient restClient;

	public SocialGuardClient(RestClient restClient) {
		this.restClient = restClient;
	}

	public BatchResponse getBatch(UUID batchId) {
		return call(() -> restClient.get()
				.uri("/api/v1/batches/{id}", batchId)
				.retrieve()
				.body(BatchResponse.class), "Could not retrieve batch " + batchId);
	}

	public boolean claimBatch(UUID batchId) {
		ClaimResponse response = call(() -> restClient.post()
				.uri("/api/v1/batches/{id}/claim", batchId)
				.retrieve()
				.body(ClaimResponse.class), "Could not claim batch " + batchId);
		if (response == null) {
			throw new IllegalStateException("SocialGuard returned an empty claim response for batch " + batchId);
		}
		return response.claimed();
	}

	public void submitResults(UUID batchId, BatchResultsRequest results) {
		call(() -> restClient.put()
				.uri("/api/v1/batches/{id}/results", batchId)
				.body(results)
				.retrieve()
				.toBodilessEntity(), "Could not submit results for batch " + batchId);
	}

	public void reportFailure(UUID batchId, String reason) {
		call(() -> restClient.post()
				.uri("/api/v1/batches/{id}/failure", batchId)
				.body(new FailureRequest(reason))
				.retrieve()
				.toBodilessEntity(), "Could not report failure for batch " + batchId);
	}

	private <T> T call(RestCall<T> operation, String context) {
		try {
			return operation.execute();
		}
		catch (RestClientResponseException exception) {
			HttpStatusCode status = exception.getStatusCode();
			if (status.is4xxClientError()) {
				throw new PermanentBatchProcessingException(
						context + " (HTTP " + status.value() + ")", exception);
			}
			throw exception;
		}
	}

	@FunctionalInterface
	private interface RestCall<T> {
		T execute();
	}
}
