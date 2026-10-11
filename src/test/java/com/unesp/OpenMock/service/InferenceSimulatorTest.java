package com.unesp.OpenMock.service;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.unesp.OpenMock.configuration.BatchProcessingProperties;

class InferenceSimulatorTest {

	private final InferenceSimulator simulator = new InferenceSimulator(new BatchProcessingProperties(
			"exchange", "queue", "key", "dlx", "dlq", "failed",
			new BatchProcessingProperties.Retry(3, Duration.ofSeconds(1), 2, Duration.ofSeconds(5)),
			new BatchProcessingProperties.Inference(100, 500, List.of("gpt-4", "gpt-3.5-turbo"))));

	@Test
	void generatesOneValidResultPerCommentAndAggregatesTokenConsumption() {
		var batch = new BatchContracts.BatchResponse(List.of(
				new BatchContracts.CommentResponse(12L),
				new BatchContracts.CommentResponse(13L),
				new BatchContracts.CommentResponse(14L)));

		BatchContracts.BatchResultsRequest results = simulator.generate(batch);

		assertEquals(List.of(12L, 13L, 14L), results.comments().stream().map(BatchContracts.CommentResult::id).toList());
		assertTrue(results.comments().stream()
				.allMatch(comment -> List.of("OK", "SPAM", "VIOLENT", "SEXUAL").contains(comment.inferenceResult())));
		assertTrue(results.tokensConsumed() >= 300);
		assertTrue(results.tokensConsumed() <= 1500);
		assertTrue(List.of("gpt-4", "gpt-3.5-turbo").contains(results.llmModel()));
	}

	@Test
	void rejectsMissingCommentsInsteadOfProducingAnInvalidPayload() {
		assertThrows(PermanentBatchProcessingException.class, () -> simulator.generate(new BatchContracts.BatchResponse(null)));
	}
}
