package net.csini.spring.kafka.producer;

import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import net.csini.spring.kafka.KafkaEntityException;

//@FunctionalInterface
public interface KafkaEntityFutureMaker<T> {
	/**
	 * 
	 * @param kafkaEntity
	 * @return CompetableFuture<RecordMetadata.timestamp>
	 * @throws KafkaEntityException
	 */
	public CompletableFuture<LocalDateTime> call(T event);

	public CompletableFuture<Void> call(Stream<T> events);

}