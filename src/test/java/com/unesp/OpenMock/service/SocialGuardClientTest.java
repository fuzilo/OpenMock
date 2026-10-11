package com.unesp.OpenMock.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.*;
import static org.springframework.test.web.client.response.MockRestResponseCreators.*;

import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

import com.unesp.OpenMock.service.BatchContracts.BatchResultsRequest;
import com.unesp.OpenMock.service.BatchContracts.CommentResult;

class SocialGuardClientTest {

	@Test
	void preservesUuidInAllSocialGuardEndpointPaths() {
		UUID batchId = UUID.fromString("b9e1aab3-6ff1-401d-8eee-ee19ab04b468");
		String batchUrl = "http://localhost:8084/api/v1/batches/" + batchId;
		var builder = RestClient.builder().baseUrl("http://localhost:8084");
		var server = MockRestServiceServer.bindTo(builder).build();
		var client = new SocialGuardClient(builder.build());

		server.expect(requestTo(batchUrl)).andExpect(method(HttpMethod.GET))
				.andRespond(withSuccess("{\"comments\":[{\"id\":12}]}", MediaType.APPLICATION_JSON));
		server.expect(requestTo(batchUrl + "/claim")).andExpect(method(HttpMethod.POST))
				.andRespond(withSuccess("{\"claimed\":true}", MediaType.APPLICATION_JSON));
		server.expect(requestTo(batchUrl + "/results")).andExpect(method(HttpMethod.PUT))
				.andExpect(content().json("""
						{"comments":[{"id":12,"inferenceResult":"SPAM"}],
						 "tokensConsumed":150,"llmModel":"gpt-4"}
						"""))
				.andRespond(withNoContent());
		server.expect(requestTo(batchUrl + "/failure")).andExpect(method(HttpMethod.POST))
				.andRespond(withNoContent());

		assertEquals(12L, client.getBatch(batchId).comments().getFirst().id());
		assertTrue(client.claimBatch(batchId));
		client.submitResults(batchId,
				new BatchResultsRequest(List.of(new CommentResult(12L, "SPAM")), 150, "gpt-4"));
		client.reportFailure(batchId, "Processing failed");
		server.verify();
	}
}
