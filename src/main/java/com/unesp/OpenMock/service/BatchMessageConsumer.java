package com.unesp.OpenMock.service;

import static com.unesp.OpenMock.service.BatchContracts.*;

import java.util.UUID;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

@Component
public class BatchMessageConsumer {

	private final SocialGuardClient socialGuardClient;
	private final InferenceSimulator inferenceSimulator;
	private final RetryTemplate retryTemplate;

	public BatchMessageConsumer(
			SocialGuardClient socialGuardClient,
			InferenceSimulator inferenceSimulator,
			RetryTemplate batchRetryTemplate) {
		this.socialGuardClient = socialGuardClient;
		this.inferenceSimulator = inferenceSimulator;
		this.retryTemplate = batchRetryTemplate;
	}

	@RabbitListener(queues = "${openmock.batch.queue}")
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

		try {
			BatchResponse batch = retryTemplate.execute(context -> socialGuardClient.getBatch(batchId));
			if (!retryTemplate.execute(context -> socialGuardClient.claimBatch(batchId))) {
				return;
			}
			BatchResultsRequest results = inferenceSimulator.generate(batch);
			retryTemplate.execute(context -> {
				socialGuardClient.submitResults(batchId, results);
				return null;
			});
		}
		catch (PermanentBatchProcessingException exception) {
			retryTemplate.execute(context -> {
				socialGuardClient.reportFailure(batchId, exception.getMessage());
				return null;
			});
		}
	}
}
