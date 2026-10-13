package com.unesp.OpenMock.service;

import static com.unesp.OpenMock.service.BatchContracts.*;

public interface InferenceClient {

	/**
	 * @throws InferenceUnavailableException when the provider fails in a way that may succeed later
	 * @throws PermanentBatchProcessingException when the batch itself cannot be classified
	 */
	BatchResultsRequest generate(BatchResponse batch);
}
