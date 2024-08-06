package net.csini.spring.kafka.mapping;

import org.springframework.kafka.support.mapping.Jackson2JavaTypeMapper;
import org.springframework.kafka.support.serializer.JsonDeserializer;

public class JsonKeyDeserializer<T> extends JsonDeserializer<T> {
	
	@Override
	public Jackson2JavaTypeMapper getTypeMapper() {
		return new DefaultJackson2JavaKeyTypeMapper();
	}

	public JsonKeyDeserializer(Class<? super T> targetType) {
		super(targetType);
		setTypeMapper(new DefaultJackson2JavaKeyTypeMapper());
	}

}
