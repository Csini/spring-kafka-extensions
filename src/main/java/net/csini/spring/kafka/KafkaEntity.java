package net.csini.spring.kafka;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * define per topic one class or record and mark with @KafkaEntity (mark a field
 * with @KafkaEntityKey)
 * 
 * Topic must exists.
 * 
 * Topic name will be the name of the entity class with included packagename but
 * you can use custom Topic name like
 * this @KafkaEntity(customTopicName="PRODUCT")
 * 
 * @author Csini
 */
@Target({ ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
public @interface KafkaEntity {

//	readonly true/false

	/**
	 * Topic name will be the name of the entity class with included packagename
	 * unless you use this property
	 * 
	 * @return custom topic name
	 */
	String customTopicName() default "";
}
