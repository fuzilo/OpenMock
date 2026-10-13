package com.unesp.OpenMock.configuration;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.unesp.OpenMock.service.InferenceUnavailableException;
import com.unesp.OpenMock.service.PermanentBatchProcessingException;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

class InferenceCircuitBreakerConfigurationTest {

	private final InferenceCircuitBreakerConfiguration configuration = new InferenceCircuitBreakerConfiguration();
	private final CircuitBreaker circuitBreaker = circuitBreaker(Duration.ofSeconds(30));

	private CircuitBreaker circuitBreaker(Duration waitDurationInOpenState) {
		var properties = new BatchProcessingProperties(
				"exchange", "queue", "key", "dlx", "dlq", "failed",
				new BatchProcessingProperties.Retry(3, Duration.ofSeconds(1), 2, Duration.ofSeconds(5)),
				new BatchProcessingProperties.Inference(100, 500, List.of("gpt-4"), 0, Duration.ZERO,
						new BatchProcessingProperties.CircuitBreaker(
								50, 4, 4, waitDurationInOpenState, 1, Duration.ofSeconds(30), 50)));
		return configuration.inferenceCircuitBreaker(configuration.circuitBreakerRegistry(properties));
	}

	private static void fail(CircuitBreaker circuitBreaker, RuntimeException exception) {
		assertThrows(exception.getClass(), () -> circuitBreaker.executeRunnable(() -> {
			throw exception;
		}));
	}

	@Test
	void opensWhenTheFailureRateReachesTheThresholdAndBlocksFurtherCalls() {
		circuitBreaker.executeRunnable(() -> { });
		circuitBreaker.executeRunnable(() -> { });
		fail(circuitBreaker, new InferenceUnavailableException("unavailable"));
		assertEquals(CircuitBreaker.State.CLOSED, circuitBreaker.getState());
		fail(circuitBreaker, new InferenceUnavailableException("unavailable"));

		assertEquals(CircuitBreaker.State.OPEN, circuitBreaker.getState());
		assertThrows(CallNotPermittedException.class, () -> circuitBreaker.executeRunnable(() -> { }));
	}

	@Test
	void doesNotCountPermanentBatchErrorsAsProviderFailures() {
		for (int call = 0; call < 10; call++) {
			fail(circuitBreaker, new PermanentBatchProcessingException("invalid batch"));
		}

		assertEquals(CircuitBreaker.State.CLOSED, circuitBreaker.getState());
	}

	@Test
	void movesToHalfOpenOnItsOwnAfterTheWaitDuration() throws InterruptedException {
		var shortWait = circuitBreaker(Duration.ofMillis(50));
		shortWait.transitionToOpenState();

		for (int attempt = 0; attempt < 100 && shortWait.getState() == CircuitBreaker.State.OPEN; attempt++) {
			Thread.sleep(20);
		}

		assertEquals(CircuitBreaker.State.HALF_OPEN, shortWait.getState());
	}

	@Test
	void exposesTheCircuitStateAsAMetric() {
		var meterRegistry = new SimpleMeterRegistry();
		var registry = configuration.circuitBreakerRegistry(new BatchProcessingProperties(
				"exchange", "queue", "key", "dlx", "dlq", "failed", null,
				new BatchProcessingProperties.Inference(100, 500, List.of("gpt-4"), 0, Duration.ZERO,
						new BatchProcessingProperties.CircuitBreaker(
								50, 4, 4, Duration.ofSeconds(30), 1, Duration.ofSeconds(30), 50))));
		configuration.inferenceCircuitBreaker(registry);
		TaggedCircuitBreakerMetrics metrics = configuration.circuitBreakerMetrics(registry);

		metrics.bindTo(meterRegistry);

		assertEquals(1, meterRegistry.get("resilience4j.circuitbreaker.state")
				.tag("name", "openai").tag("state", "closed").gauge().value());
	}
}
