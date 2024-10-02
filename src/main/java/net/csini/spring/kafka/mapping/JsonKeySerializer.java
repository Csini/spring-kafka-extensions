package net.csini.spring.kafka.mapping;

import org.springframework.kafka.support.mapping.Jackson2JavaTypeMapper;
import org.springframework.kafka.support.serializer.JsonSerializer;

/**
 * same as JsonSerializer only using DefaultJackson2JavaKeyTypeMapper
 * 
 * @param <T> class of the entity, representing messages
 * 
 * @author Csini
 */
public class JsonKeySerializer<T> extends JsonSerializer<T> {

	@Override
	public Jackson2JavaTypeMapper getTypeMapper() {
		return new DefaultJackson2JavaKeyTypeMapper();
	}

	/**
	 * default constructor, use this
	 */
	public JsonKeySerializer() {
		super();
		setTypeMapper(new DefaultJackson2JavaKeyTypeMapper());
	}

}