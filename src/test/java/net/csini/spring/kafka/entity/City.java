package net.csini.spring.kafka.entity;

import net.csini.spring.kafka.KafkaEntity;
import net.csini.spring.kafka.Key;

@KafkaEntity
public record City(@Key String name) {

	public String name() {
		return name;
	}
}
