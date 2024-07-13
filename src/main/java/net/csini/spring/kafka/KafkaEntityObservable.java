package net.csini.spring.kafka;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface KafkaEntityObservable {

	Class entity();

	String groupid();

//	  consumer:
//	      group-id: siTestGroup
//	      auto-offset-reset: earliest
//	      enable-auto-commit: false
//	      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
//	      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer

//	int batchSize() default 0;
}
