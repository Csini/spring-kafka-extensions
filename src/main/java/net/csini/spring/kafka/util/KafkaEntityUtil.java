package net.csini.spring.kafka.util;

import org.apache.commons.lang3.StringUtils;

import net.csini.spring.kafka.KafkaEntity;

public class KafkaEntityUtil {

	public static String getTopicName(Class entity) {
		KafkaEntity topic = extractKafkaEntity(entity);
		return getTopicName(entity, topic);
	}

	public static String getTopicName(Class entity, KafkaEntity topic) {
		if (!StringUtils.isEmpty(topic.customTopicName())) {
			return topic.customTopicName();
		}
		return entity.getName();
	}

	public static KafkaEntity extractKafkaEntity(Class entity) {
		return (KafkaEntity) entity.getAnnotation(KafkaEntity.class);
	}
}
