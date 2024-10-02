package net.csini.spring.kafka.util;

import org.apache.commons.lang3.StringUtils;

import net.csini.spring.kafka.KafkaEntity;

/**
 * Kafka Entity Utility Class
 * 
 * @author Csini
 */
public class KafkaEntityUtil {

	private KafkaEntityUtil() {
		super();
	}

	/**
	 * Gets the name of the topic to a @KafkaEntity
	 * 
	 * @param entity class which is marked with @KafkaEntity
	 * @return the name of the entity class with included packagename but you can
	 *         use custom Topic name like
	 *         this @KafkaEntity(customTopicName="PRODUCT")
	 */
	public static String getTopicName(Class entity) {
		KafkaEntity topic = extractKafkaEntity(entity);
		return getTopicName(entity, topic);
	}

	private static String getTopicName(Class entity, KafkaEntity topic) {
		if (!StringUtils.isEmpty(topic.customTopicName())) {
			return topic.customTopicName();
		}
		return entity.getName();
	}

	private static KafkaEntity extractKafkaEntity(Class entity) {
		return (KafkaEntity) entity.getAnnotation(KafkaEntity.class);
	}
}
