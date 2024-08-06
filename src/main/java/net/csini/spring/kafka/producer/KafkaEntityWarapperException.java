package net.csini.spring.kafka.producer;

import lombok.Getter;
import net.csini.spring.kafka.KafkaEntityException;

@Getter
public class KafkaEntityWarapperException extends RuntimeException {

	private KafkaEntityException wrapped;

	public KafkaEntityWarapperException(KafkaEntityException wrapped) {
		super(wrapped);
		this.wrapped = wrapped;
	}

}
