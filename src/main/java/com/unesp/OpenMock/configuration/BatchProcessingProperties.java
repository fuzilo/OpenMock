package com.unesp.OpenMock.configuration;

import java.time.Duration;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("openmock.batch")
public record BatchProcessingProperties(
		String exchange,
		String queue,
		String routingKey,
		String dlx,
		String dlq,
		String dlxRoutingKey,
		Retry retry,
		Inference inference) {

	public record Retry(int maxAttempts, Duration initialInterval, double multiplier, Duration maxInterval) {
	}

	public record Inference(
			int minimumTokens,
			int maximumTokens,
			List<String> models,
			double simulatedFailureRate,
			Duration simulatedLatency,
			CircuitBreaker circuitBreaker) {
	}

	public record CircuitBreaker(
			float failureRateThreshold,
			int slidingWindowSize,
			int minimumNumberOfCalls,
			Duration waitDurationInOpenState,
			int permittedCallsInHalfOpenState,
			Duration slowCallDurationThreshold,
			float slowCallRateThreshold) {
	}
}
