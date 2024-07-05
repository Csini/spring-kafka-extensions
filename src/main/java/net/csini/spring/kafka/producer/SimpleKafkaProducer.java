package net.csini.spring.kafka.producer;

import java.util.concurrent.CompletableFuture;

import net.csini.spring.kafka.KafkaEntityException;

public interface SimpleKafkaProducer<T>{

	/**
	 * 
	 * @param event
	 * @return CompetableFuture<RecordMetadata.timestamp>
	 * @throws KafkaEntityException
	 */
	public CompletableFuture<Long> send(T event) throws KafkaEntityException;

}
