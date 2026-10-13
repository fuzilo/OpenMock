package com.unesp.OpenMock.configuration;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.unesp.OpenMock.service.PermanentBatchProcessingException;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig.SlidingWindowType;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics;

@Configuration
@EnableConfigurationProperties(BatchProcessingProperties.class)
public class InferenceCircuitBreakerConfiguration {

	public static final String CIRCUIT_BREAKER_NAME = "openai";

	@Bean
	CircuitBreakerRegistry circuitBreakerRegistry(BatchProcessingProperties properties) {
		var circuitBreaker = properties.inference().circuitBreaker();
		return CircuitBreakerRegistry.of(CircuitBreakerConfig.custom()
				.slidingWindowType(SlidingWindowType.COUNT_BASED)
				.slidingWindowSize(circuitBreaker.slidingWindowSize())
				.minimumNumberOfCalls(circuitBreaker.minimumNumberOfCalls())
				.failureRateThreshold(circuitBreaker.failureRateThreshold())
				.slowCallDurationThreshold(circuitBreaker.slowCallDurationThreshold())
				.slowCallRateThreshold(circuitBreaker.slowCallRateThreshold())
				.waitDurationInOpenState(circuitBreaker.waitDurationInOpenState())
				.permittedNumberOfCallsInHalfOpenState(circuitBreaker.permittedCallsInHalfOpenState())
				// The listener is stopped while the circuit is open, so no call would ever trigger the transition.
				.automaticTransitionFromOpenToHalfOpenEnabled(true)
				.ignoreExceptions(PermanentBatchProcessingException.class)
				.build());
	}

	@Bean
	CircuitBreaker inferenceCircuitBreaker(CircuitBreakerRegistry circuitBreakerRegistry) {
		return circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME);
	}

	@Bean
	TaggedCircuitBreakerMetrics circuitBreakerMetrics(CircuitBreakerRegistry circuitBreakerRegistry) {
		return TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(circuitBreakerRegistry);
	}
}
