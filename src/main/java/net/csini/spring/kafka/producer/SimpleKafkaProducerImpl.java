package net.csini.spring.kafka.producer;

import java.util.concurrent.CompletableFuture;

public class SimpleKafkaProducerImpl<T> /* extends KafkaTemplate<K, T> **/ implements SimpleKafkaProducer<T> {

	private Class<T> clazz;

	public SimpleKafkaProducerImpl(Class<T> clazz) {
		this.clazz = clazz;
	}

	public Class<T> getClazz() {
		return this.clazz;
	}

//	public CompletableFuture<SendResult<T>> send(T event) {
//		//TODO
//		return null;
//	}

	public CompletableFuture<Void> send(T event) {
		// TODO

		System.out.println("sending: " + event);
		return null;
	}

}
