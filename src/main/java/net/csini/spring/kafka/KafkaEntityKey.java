package net.csini.spring.kafka;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * define per topic one class or record and mark with @KafkaEntity (mark a field
 * with @KafkaEntityKey)
 * 
 * @author Csini
 */
@Target({ ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
public @interface KafkaEntityKey {

}
