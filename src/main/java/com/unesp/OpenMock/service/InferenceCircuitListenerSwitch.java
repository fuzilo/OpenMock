package com.unesp.OpenMock.service;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.stereotype.Component;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreaker.State;

/**
 * Stops the batch listener while the inference circuit is open, so messages wait in the queue
 * instead of being requeued in a loop, and starts it again when the circuit lets calls through.
 */
@Component
public class InferenceCircuitListenerSwitch {

	// The transition to open is published on the listener thread, which cannot stop its own container
	// without waiting for itself; a single thread also keeps stop and start in transition order.
	private final ExecutorService executor = Executors.newSingleThreadExecutor(
			Thread.ofPlatform().daemon().name("inference-circuit-listener-switch").factory());

	public InferenceCircuitListenerSwitch(
			CircuitBreaker inferenceCircuitBreaker,
			RabbitListenerEndpointRegistry listenerRegistry) {
		inferenceCircuitBreaker.getEventPublisher().onStateTransition(event -> {
			State state = event.getStateTransition().getToState();
			executor.execute(() -> {
				var container = listenerRegistry.getListenerContainer(BatchMessageConsumer.LISTENER_ID);
				if (container == null) {
					return;
				}
				if (state == State.OPEN || state == State.FORCED_OPEN) {
					container.stop();
				}
				else {
					container.start();
				}
			});
		});
	}
}
