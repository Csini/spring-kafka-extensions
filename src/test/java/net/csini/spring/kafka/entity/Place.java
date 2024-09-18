package net.csini.spring.kafka.entity;

import net.csini.spring.kafka.KafkaEntity;
import net.csini.spring.kafka.KafkaEntityKey;

@KafkaEntity
public record Place(@KafkaEntityKey String id) {

	public String id() {
		return id;
	}
}
