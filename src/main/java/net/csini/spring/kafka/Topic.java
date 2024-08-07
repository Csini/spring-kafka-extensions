package net.csini.spring.kafka;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface Topic {

	String name();
	
	boolean autoCreate() default false;
	
	int numPartitions() default 1;
	
	short replicationFactor() default 0;

}
