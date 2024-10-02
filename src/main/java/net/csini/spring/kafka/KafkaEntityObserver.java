package net.csini.spring.kafka;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * to write data to a topic (produce) in a Spring Bean just inject a
 * KafkaEntityObserver
 * 
 * default is transactional=true
 * 
 * @author Csini
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface KafkaEntityObserver {

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
