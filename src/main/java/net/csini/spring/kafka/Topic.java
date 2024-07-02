package net.csini.spring.kafka;

public @interface Topic {

	String name();
	
	boolean autoCreate() default false;
	
	int numPartitions() default 1;
	
	short replicationFactor() default 0;

}
