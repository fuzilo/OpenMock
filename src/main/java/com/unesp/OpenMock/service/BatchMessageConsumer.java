package com.unesp.OpenMock.service;

import static com.unesp.OpenMock.service.BatchContracts.*;

import java.util.UUID;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.ImmediateRequeueAmqpException;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreaker.State;

@Component
public class BatchMessageConsumer {

	public static final String LISTENER_ID = "batchMessageConsumer";

	private final SocialGuardClient socialGuardClient;
	private final InferenceClient inferenceClient;
	private final RetryTemplate retryTemplate;
	private final CircuitBreaker inferenceCircuitBreaker;

	public BatchMessageConsumer(
			SocialGuardClient socialGuardClient,
			InferenceClient inferenceClient,
			RetryTemplate batchRetryTemplate,
			CircuitBreaker inferenceCircuitBreaker) {
		this.socialGuardClient = socialGuardClient;
		this.inferenceClient = inferenceClient;
		this.retryTemplate = batchRetryTemplate;
		this.inferenceCircuitBreaker = inferenceCircuitBreaker;
	}

	@RabbitListener(id = LISTENER_ID, queues = "${openmock.batch.queue}")
	public void consume(String message) {
		UUID batchId;
		try {
			String value = message == null ? "" : message.trim();
			batchId = UUID.fromString(value);
			if (!batchId.toString().equalsIgnoreCase(value)) {
				throw new IllegalArgumentException("batchId must be a canonical UUID");
			}
		}
		catch (IllegalArgumentException exception) {
			throw new AmqpRejectAndDontRequeueException("Invalid batchId message", exception);
		}

		// Checked before the claim so a batch is never claimed while inference is unavailable.
		State circuitState = inferenceCircuitBreaker.getState();
		if (circuitState == State.OPEN || circuitState == State.FORCED_OPEN) {
			throw new ImmediateRequeueAmqpException("Inference circuit is open, batch " + batchId + " requeued");
		}

		try {
			BatchResponse batch = retryTemplate.execute(context -> socialGuardClient.getBatch(batchId));
			if (!retryTemplate.execute(context -> socialGuardClient.claimBatch(batchId))) {
				return;
			}
			BatchResultsRequest results = inferenceCircuitBreaker.executeSupplier(
					() -> inferenceClient.generate(batch));
			retryTemplate.execute(context -> {
				socialGuardClient.submitResults(batchId, results);
				return null;
			});
		}
		// Inference failures happen after the claim, so they are reported instead of leaving the batch claimed.
		catch (PermanentBatchProcessingException | InferenceUnavailableException
				| CallNotPermittedException exception) {
			retryTemplate.execute(context -> {
				socialGuardClient.reportFailure(batchId, exception.getMessage());
				return null;
			});
		}
	}
}
