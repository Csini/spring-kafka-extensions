package net.csini.spring.kafka.producer;

import java.util.concurrent.CompletableFuture;

public interface SimpleKafkaProducer<T>{

	public CompletableFuture<Void> send(T event);

}
