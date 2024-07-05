package net.csini.spring.kafka;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface KafkaEntityProducer {

	Class entity();
//	    producer:
//	      batch-size: 16384
//	      buffer-memory: 33554432
//	      retries: 0
//	      key-serializer: org.apache.kafka.common.serialization.StringSerializer
//	      value-serializer: org.apache.kafka.common.serialization.StringSerializer

	String clientid();
}
