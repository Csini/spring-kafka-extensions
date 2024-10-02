package net.csini.spring.kafka;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * to write data to a topic (produce)
 * 
 * if you need the RecordMetadata from the Kafka Message than you can use
 * KafkaEntitySubject
 * 
 * in a Spring Bean just inject a KafkaEntitySubject
 * 
 * default is transactional=true
 * 
 * @author Csini
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface KafkaEntitySubject {

	/**
	 * Kafka Entity Class
	 * 
	 * @return entity class
	 */
	Class entity();

	/**
	 * should the Kafka Producer create a Kafka Tranaction? default true
	 * 
	 * @return transactional
	 */
	boolean transactional() default true;
}
