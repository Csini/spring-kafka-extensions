package net.csini.spring.kafka.mapping;

import org.springframework.kafka.support.mapping.DefaultJackson2JavaTypeMapper;

public class DefaultJackson2JavaKeyTypeMapper extends DefaultJackson2JavaTypeMapper {

	@Override
	public String getClassIdFieldName() {
		return "__Key_TypeId__";
	}

}
