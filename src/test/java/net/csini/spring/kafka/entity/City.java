package net.csini.spring.kafka.entity;

import net.csini.spring.kafka.KafkaEntity;
import net.csini.spring.kafka.KafkaEntityKey;

@KafkaEntity
public record City(@KafkaEntityKey String name) {

	public String name() {
		return name;
	}
}
