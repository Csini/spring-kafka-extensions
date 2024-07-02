package net.csini.spring.kafka;

public @interface KafkaEntityListener {

	Class entity();
	
	int batchSize() default 0;
//	  consumer:
//	      group-id: siTestGroup
//	      auto-offset-reset: earliest
//	      enable-auto-commit: false
//	      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
//	      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
	    
}
