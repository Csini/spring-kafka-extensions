package net.csini.spring.kafka.producer;

import java.util.concurrent.CompletableFuture;

import net.csini.spring.kafka.KafkaEntityException;

public interface SimpleKafkaProducer<T>{

	public CompletableFuture<Void> send(T event) throws KafkaEntityException;

}
