package net.csini.spring.kafka.producer;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import net.csini.spring.kafka.KafkaEntityException;
import net.csini.spring.kafka.KafkaEntityProducer;
import net.csini.spring.kafka.Key;
import net.csini.spring.kafka.Topic;

public class SimpleKafkaProducer<T, K> /* extends KafkaTemplate<K, T> **/ implements KafkaEntityFuture<T> {

	private static final Logger LOGGER = LoggerFactory.getLogger(SimpleKafkaProducer.class);

	/**
	 * Comma-delimited list of host:port pairs to use for establishing the initial
	 * connections to the Kafka cluster. Applies to all components unless
	 * overridden.
	 */
	@Value(value = "${spring.kafka.bootstrap-servers:localhost:9092}")
	private List<String> bootstrapServers = new ArrayList<>(Collections.singletonList("localhost:9092"));

	private String clientid;

	@Bean
	public ProducerFactory<K, T> simpleProducerFactory() {
		Map<String, Object> configProps = new HashMap<>();
		configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		configProps.put(ProducerConfig.CLIENT_ID_CONFIG, clientid);
		return new DefaultKafkaProducerFactory<>(configProps);
	}

	@Bean
	public KafkaTemplate<K, T> simpleKafkaTemplate() {
		return new KafkaTemplate<>(simpleProducerFactory());
	}

	private Class<T> clazz;

	public SimpleKafkaProducer(KafkaEntityProducer kafkaEntityProducer) {
		this.clazz = kafkaEntityProducer.entity();
		this.clientid = kafkaEntityProducer.clientid();
	}

	public Class<T> getClazz() {
		return this.clazz;
	}

//	public CompletableFuture<SendResult<T>> send(T event) {
//		//TODO
//		return null;
//	}

	public CompletableFuture<Long> call(T event) throws KafkaEntityException {

		LOGGER.info("sending: " + event);
		String topic = getTopicName();

		K key = extractKey(event);

		return simpleKafkaTemplate().send(topic, key, event)
				.thenApply(sendresult -> sendresult.getRecordMetadata().timestamp());
	}

	private String getTopicName() {
		Topic topic = extractTopic();
		if (topic != null) {
			return topic.name();
		}
		return getClazz().getName();
	}

	private Topic extractTopic() {
		return getClazz().getAnnotation(Topic.class);
	}

	//TODO place in constructor to find the field
	private K extractKey(T event) throws KafkaEntityException {
		for (Field field : getClazz().getDeclaredFields()) {
			LOGGER.debug("    field  -> " + field.getName());
			if (field.isAnnotationPresent(Key.class)) {

				Key key = field.getAnnotation(Key.class);

				try {
					field.setAccessible(true);
					return (K) field.get(event);
				} catch (IllegalArgumentException | IllegalAccessException e) {
					throw new KafkaEntityException(e);
				}
			}
		}

		throw new KafkaEntityException("@Key is mandatory in @KafkaEntity");
	}

}
