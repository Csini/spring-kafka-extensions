package net.csini.spring.kafka.mapping;

import org.springframework.kafka.support.mapping.Jackson2JavaTypeMapper;
import org.springframework.kafka.support.serializer.JsonSerializer;

public class JsonKeySerializer<T> extends JsonSerializer<T> {

	@Override
	public Jackson2JavaTypeMapper getTypeMapper() {
		return new DefaultJackson2JavaKeyTypeMapper();
	}

	public JsonKeySerializer() {
		super();
		setTypeMapper(new DefaultJackson2JavaKeyTypeMapper());
	}

}