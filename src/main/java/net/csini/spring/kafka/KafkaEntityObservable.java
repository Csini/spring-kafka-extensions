package net.csini.spring.kafka;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * to read data from a topic (consume) in a Spring Bean just inject a
 * KafkaEntityObservable
 * 
 * @author Csini
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface KafkaEntityObservable {

	/**
	 * Kafka Entity Class
	 * 
	 * @return entity class
	 */
	Class entity();

}
