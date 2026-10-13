package com.unesp.OpenMock.service;

import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.MessageListenerContainer;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;

class InferenceCircuitListenerSwitchTest {

	private final MessageListenerContainer container = mock(MessageListenerContainer.class);
	private final RabbitListenerEndpointRegistry listenerRegistry = mock(RabbitListenerEndpointRegistry.class);
	private final CircuitBreaker circuitBreaker = CircuitBreaker.ofDefaults("openai");

	@Test
	void stopsTheListenerWhenTheCircuitOpensAndStartsItAgainWhenItHalfOpens() {
		when(listenerRegistry.getListenerContainer(BatchMessageConsumer.LISTENER_ID)).thenReturn(container);
		new InferenceCircuitListenerSwitch(circuitBreaker, listenerRegistry);

		circuitBreaker.transitionToOpenState();
		circuitBreaker.transitionToHalfOpenState();

		var order = inOrder(container);
		order.verify(container, timeout(2000)).stop();
		order.verify(container, timeout(2000)).start();
	}
}
