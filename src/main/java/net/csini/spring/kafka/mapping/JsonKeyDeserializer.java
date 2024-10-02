package net.csini.spring.kafka.mapping;

import org.springframework.kafka.support.mapping.Jackson2JavaTypeMapper;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * same as JsonDeserializer only the classIdFieldName is other because using
 * DefaultJackson2JavaKeyTypeMapper
 * 
 * @param <T> class of the entity, representing messages
 * 
 * @author Csini
 */
public class JsonKeyDeserializer<T> extends JsonDeserializer<T> {

	@Override
	public Jackson2JavaTypeMapper getTypeMapper() {
		return new DefaultJackson2JavaKeyTypeMapper();
	}

	/**
	 * Construct an instance with the provided target type
	 * 
	 * @param targetType the target type to use if no type info headers are present.
	 */
	public JsonKeyDeserializer(Class<? super T> targetType) {
		super(targetType);
		setTypeMapper(new DefaultJackson2JavaKeyTypeMapper());
	}

}
