package net.csini.spring.kafka.entity;

import net.csini.spring.kafka.KafkaEntity;

@KafkaEntity
public record User(String userid) {

}
