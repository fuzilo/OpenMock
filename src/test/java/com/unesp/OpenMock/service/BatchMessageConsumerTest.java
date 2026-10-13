package com.unesp.OpenMock.service;

import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.ImmediateRequeueAmqpException;
import org.springframework.retry.support.RetryTemplate;

import com.unesp.OpenMock.service.BatchContracts.BatchResponse;
import com.unesp.OpenMock.service.BatchContracts.BatchResultsRequest;
import com.unesp.OpenMock.service.BatchContracts.CommentResponse;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;

class BatchMessageConsumerTest {

	private static final UUID BATCH_ID = UUID.fromString("b9e1aab3-6ff1-401d-8eee-ee19ab04b468");

	private final SocialGuardClient socialGuardClient = mock(SocialGuardClient.class);
	private final InferenceClient inferenceClient = mock(InferenceClient.class);
	private final CircuitBreaker circuitBreaker = CircuitBreaker.of("openai", CircuitBreakerConfig.custom()
			.slidingWindowSize(2)
			.minimumNumberOfCalls(2)
			.ignoreExceptions(PermanentBatchProcessingException.class)
			.build());
	private final BatchMessageConsumer consumer =
			new BatchMessageConsumer(socialGuardClient, inferenceClient, new RetryTemplate(), circuitBreaker);

	@Test
	void acknowledgesDuplicateWithoutGeneratingOrSubmittingResults() {
		var batch = new BatchResponse(List.of(new CommentResponse(12L)));
		when(socialGuardClient.getBatch(BATCH_ID)).thenReturn(batch);
		when(socialGuardClient.claimBatch(BATCH_ID)).thenReturn(false);

		consumer.consume(BATCH_ID.toString());

		verify(inferenceClient, never()).generate(batch);
		verify(socialGuardClient, never()).submitResults(eq(BATCH_ID), any());
	}

	@Test
	void submitsResultsOnlyAfterSuccessfulClaim() {
		var batch = new BatchResponse(List.of(new CommentResponse(12L)));
		var results = new BatchResultsRequest(List.of(), 100, "gpt-4");
		when(socialGuardClient.getBatch(BATCH_ID)).thenReturn(batch);
		when(socialGuardClient.claimBatch(BATCH_ID)).thenReturn(true);
		when(inferenceClient.generate(batch)).thenReturn(results);

		consumer.consume(BATCH_ID.toString());

		verify(socialGuardClient).submitResults(BATCH_ID, results);
	}

	@Test
	void reportsPermanentProcessingErrorsToSocialGuard() {
		when(socialGuardClient.getBatch(BATCH_ID))
				.thenThrow(new PermanentBatchProcessingException("SocialGuard rejected the batch"));

		consumer.consume(BATCH_ID.toString());

		verify(socialGuardClient).reportFailure(BATCH_ID, "SocialGuard rejected the batch");
		verify(socialGuardClient, never()).claimBatch(BATCH_ID);
	}

	@Test
	void acceptsUuidWithSurroundingWhitespace() {
		consumer.consume(" \n" + BATCH_ID + "\t ");

		verify(socialGuardClient).getBatch(BATCH_ID);
		verify(socialGuardClient).claimBatch(BATCH_ID);
	}

	@Test
	void rejectsInvalidIdentifiersWithoutCallingSocialGuard() {
		for (String message : new String[] { "42", "not-a-uuid", "", "1-1-1-1-1", null }) {
			assertThrows(AmqpRejectAndDontRequeueException.class, () -> consumer.consume(message));
		}

		verifyNoInteractions(socialGuardClient, inferenceClient);
	}

	@Test
	void reportsInferenceFailuresAfterTheClaimAndOpensTheCircuit() {
		var batch = new BatchResponse(List.of(new CommentResponse(12L)));
		when(socialGuardClient.getBatch(BATCH_ID)).thenReturn(batch);
		when(socialGuardClient.claimBatch(BATCH_ID)).thenReturn(true);
		when(inferenceClient.generate(batch)).thenThrow(new InferenceUnavailableException("OpenAI is unavailable"));

		consumer.consume(BATCH_ID.toString());
		consumer.consume(BATCH_ID.toString());

		verify(socialGuardClient, times(2)).reportFailure(BATCH_ID, "OpenAI is unavailable");
		verify(socialGuardClient, never()).submitResults(eq(BATCH_ID), any());
		assertEquals(CircuitBreaker.State.OPEN, circuitBreaker.getState());
	}

	@Test
	void requeuesWithoutCallingSocialGuardWhileTheCircuitIsOpen() {
		circuitBreaker.transitionToOpenState();

		assertThrows(ImmediateRequeueAmqpException.class, () -> consumer.consume(BATCH_ID.toString()));

		verifyNoInteractions(socialGuardClient, inferenceClient);
	}

	@Test
	void requeuesWithoutCallingSocialGuardWhileTheCircuitIsForcedOpen() {
		circuitBreaker.transitionToForcedOpenState();

		assertThrows(ImmediateRequeueAmqpException.class, () -> consumer.consume(BATCH_ID.toString()));

		verifyNoInteractions(socialGuardClient, inferenceClient);
	}

	@Test
	void reportsPermanentInferenceErrorsWithoutOpeningTheCircuit() {
		var batch = new BatchResponse(List.of(new CommentResponse(12L)));
		when(socialGuardClient.getBatch(BATCH_ID)).thenReturn(batch);
		when(socialGuardClient.claimBatch(BATCH_ID)).thenReturn(true);
		when(inferenceClient.generate(batch)).thenThrow(new PermanentBatchProcessingException("Invalid batch"));

		consumer.consume(BATCH_ID.toString());
		consumer.consume(BATCH_ID.toString());

		verify(socialGuardClient, times(2)).reportFailure(BATCH_ID, "Invalid batch");
		assertEquals(CircuitBreaker.State.CLOSED, circuitBreaker.getState());
	}

	@Test
	void reportsFailureWithoutCallingInferenceWhenTheCircuitOpensAfterTheClaim() {
		var batch = new BatchResponse(List.of(new CommentResponse(12L)));
		when(socialGuardClient.getBatch(BATCH_ID)).thenReturn(batch);
		when(socialGuardClient.claimBatch(BATCH_ID)).thenAnswer(invocation -> {
			circuitBreaker.transitionToOpenState();
			return true;
		});

		consumer.consume(BATCH_ID.toString());

		verify(socialGuardClient).reportFailure(eq(BATCH_ID), contains("is OPEN"));
		verify(socialGuardClient, never()).submitResults(eq(BATCH_ID), any());
		verifyNoInteractions(inferenceClient);
	}

	@Test
	void doesNotCountSocialGuardFailuresAgainstTheCircuit() {
		when(socialGuardClient.getBatch(BATCH_ID)).thenThrow(new IllegalStateException("SocialGuard is down"));

		assertThrows(IllegalStateException.class, () -> consumer.consume(BATCH_ID.toString()));
		assertThrows(IllegalStateException.class, () -> consumer.consume(BATCH_ID.toString()));

		assertEquals(CircuitBreaker.State.CLOSED, circuitBreaker.getState());
		assertEquals(0, circuitBreaker.getMetrics().getNumberOfFailedCalls());
		verifyNoInteractions(inferenceClient);
	}

	@Test
	void processesBatchesAgainWhenTheCircuitIsHalfOpen() {
		var batch = new BatchResponse(List.of(new CommentResponse(12L)));
		var results = new BatchResultsRequest(List.of(), 100, "gpt-4");
		when(socialGuardClient.getBatch(BATCH_ID)).thenReturn(batch);
		when(socialGuardClient.claimBatch(BATCH_ID)).thenReturn(true);
		when(inferenceClient.generate(batch)).thenReturn(results);
		circuitBreaker.transitionToOpenState();
		circuitBreaker.transitionToHalfOpenState();

		consumer.consume(BATCH_ID.toString());

		verify(socialGuardClient).submitResults(BATCH_ID, results);
	}
}
