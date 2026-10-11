package com.unesp.OpenMock.configuration;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import com.unesp.OpenMock.service.PermanentBatchProcessingException;

import java.util.Map;

@Configuration
@EnableConfigurationProperties(BatchProcessingProperties.class)
public class RabbitConfiguration {

	@Bean
	DirectExchange batchExchange(BatchProcessingProperties properties) {
		return new DirectExchange(properties.exchange(), true, false);
	}

	@Bean
	DirectExchange batchDeadLetterExchange(BatchProcessingProperties properties) {
		return new DirectExchange(properties.dlx(), true, false);
	}

	@Bean
	Queue batchQueue(BatchProcessingProperties properties) {
		return QueueBuilder.durable(properties.queue())
				.deadLetterExchange(properties.dlx())
				.deadLetterRoutingKey(properties.dlxRoutingKey())
				.build();
	}

	@Bean
	Queue batchDeadLetterQueue(BatchProcessingProperties properties) {
		return QueueBuilder.durable(properties.dlq()).build();
	}

	@Bean
	Binding batchQueueBinding(@Qualifier("batchQueue") Queue batchQueue,
			@Qualifier("batchExchange") DirectExchange batchExchange,
			BatchProcessingProperties properties) {
		return BindingBuilder.bind(batchQueue).to(batchExchange).with(properties.routingKey());
	}

	@Bean
	Binding batchDeadLetterQueueBinding(@Qualifier("batchDeadLetterQueue") Queue batchDeadLetterQueue,
			@Qualifier("batchDeadLetterExchange") DirectExchange batchDeadLetterExchange,
			BatchProcessingProperties properties) {
		return BindingBuilder.bind(batchDeadLetterQueue).to(batchDeadLetterExchange)
				.with(properties.dlxRoutingKey());
	}

	@Bean
	RetryTemplate batchRetryTemplate(BatchProcessingProperties properties) {
		var retryTemplate = RetryTemplate.builder()
				.maxAttempts(properties.retry().maxAttempts())
				.exponentialBackoff(
						properties.retry().initialInterval().toMillis(),
						properties.retry().multiplier(),
						properties.retry().maxInterval().toMillis())
				.build();
		retryTemplate.setRetryPolicy(new SimpleRetryPolicy(
				properties.retry().maxAttempts(),
				Map.of(
						PermanentBatchProcessingException.class, false,
						AmqpRejectAndDontRequeueException.class, false),
				true,
				true));
		return retryTemplate;
	}

	@Bean
	SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(ConnectionFactory connectionFactory) {
		var factory = new SimpleRabbitListenerContainerFactory();
		factory.setConnectionFactory(connectionFactory);
		factory.setDefaultRequeueRejected(false);
		return factory;
	}
}
