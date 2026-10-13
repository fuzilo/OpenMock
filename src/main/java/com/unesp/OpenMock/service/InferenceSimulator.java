package com.unesp.OpenMock.service;

import static com.unesp.OpenMock.service.BatchContracts.*;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.springframework.stereotype.Component;

import com.unesp.OpenMock.configuration.BatchProcessingProperties;

@Component
public class InferenceSimulator implements InferenceClient {

	private static final List<String> RESULTS = List.of("OK", "SPAM", "VIOLENT", "SEXUAL");

	private final BatchProcessingProperties.Inference properties;

	public InferenceSimulator(BatchProcessingProperties properties) {
		this.properties = properties.inference();
		if (properties.inference().minimumTokens() < 0
				|| properties.inference().maximumTokens() < properties.inference().minimumTokens()
				|| properties.inference().models().isEmpty()
				|| properties.inference().simulatedFailureRate() < 0
				|| properties.inference().simulatedFailureRate() > 1
				|| properties.inference().simulatedLatency().isNegative()) {
			throw new IllegalArgumentException("Invalid simulated inference configuration");
		}
	}

	@Override
	public BatchResultsRequest generate(BatchResponse batch) {
		if (batch == null || batch.comments() == null) {
			throw new PermanentBatchProcessingException("Batch response does not contain a comments list");
		}
		var random = ThreadLocalRandom.current();
		simulateProvider(random);
		var results = batch.comments().stream()
				.map(comment -> {
					if (comment.id() == null) {
						throw new PermanentBatchProcessingException("Batch contains a comment without an id");
					}
					return new CommentResult(comment.id(), RESULTS.get(random.nextInt(RESULTS.size())));
				})
				.toList();
		long tokensConsumed = batch.comments().stream()
				.mapToLong(comment -> random.nextLong(properties.minimumTokens(),
						(long) properties.maximumTokens() + 1))
				.sum();
		String model = properties.models().get(random.nextInt(properties.models().size()));
		return new BatchResultsRequest(results, tokensConsumed, model);
	}

	private void simulateProvider(ThreadLocalRandom random) {
		try {
			Thread.sleep(properties.simulatedLatency());
		}
		catch (InterruptedException exception) {
			Thread.currentThread().interrupt();
			throw new InferenceUnavailableException("Simulated inference was interrupted", exception);
		}
		if (random.nextDouble() < properties.simulatedFailureRate()) {
			throw new InferenceUnavailableException("Simulated inference provider failure");
		}
	}
}
