package net.csini.spring.kafka;

public class KafkaEntityException extends Exception {

	public KafkaEntityException(String message) {
		super(message);
	}

	public KafkaEntityException(Exception e) {
		super(e);
	}

}
