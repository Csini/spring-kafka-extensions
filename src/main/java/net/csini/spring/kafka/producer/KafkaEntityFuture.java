package net.csini.spring.kafka.producer;

import java.util.concurrent.CompletableFuture;

import net.csini.spring.kafka.KafkaEntityException;

@FunctionalInterface
public interface KafkaEntityFuture<T> {
    /**
	 * 
	 * @param kafkaEntity
	 * @return CompetableFuture<RecordMetadata.timestamp>
	 * @throws KafkaEntityException
	 */
	public CompletableFuture<Long> call(T kafkaEntity) throws KafkaEntityException;
}