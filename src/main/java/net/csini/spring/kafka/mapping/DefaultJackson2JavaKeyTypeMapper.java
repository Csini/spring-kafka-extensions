package net.csini.spring.kafka.mapping;

import org.springframework.kafka.support.mapping.DefaultJackson2JavaTypeMapper;

/**
 * same as DefaultJackson2JavaTypeMapper only the classIdFieldName is other
 * 
 * @author Csini
 */
public class DefaultJackson2JavaKeyTypeMapper extends DefaultJackson2JavaTypeMapper {

	/**
	 * default constructor, use this
	 */
	public DefaultJackson2JavaKeyTypeMapper() {
		super();
	}

	@Override
	public String getClassIdFieldName() {
		// DEFAULT_KEY_CLASSID_FIELD_NAME
		return "__Key_TypeId__";
	}

}
