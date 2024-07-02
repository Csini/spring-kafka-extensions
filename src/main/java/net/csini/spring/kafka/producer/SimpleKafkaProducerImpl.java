package net.csini.spring.kafka.producer;

import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

import net.csini.spring.kafka.KafkaEntityException;
import net.csini.spring.kafka.KafkaEntityProducer;
import net.csini.spring.kafka.Key;
import net.csini.spring.kafka.Topic;

public class SimpleKafkaProducerImpl<T, K> /* extends KafkaTemplate<K, T> **/ implements SimpleKafkaProducer<T> {

	@Autowired
	KafkaTemplate<K, T> kafkaTemplate;

	// topic

	// key

	private Class<T> clazz;

	public SimpleKafkaProducerImpl(Class<T> clazz/* , Class<K> clazzKey */) {
		this.clazz = clazz;
	}

	public Class<T> getClazz() {
		return this.clazz;
	}

//	public CompletableFuture<SendResult<T>> send(T event) {
//		//TODO
//		return null;
//	}

	public CompletableFuture<Void> send(T event) throws KafkaEntityException {
		// TODO

		System.out.println("sending: " + event);
		String topic = getTopicName(event);

		K key = extractKey(event);

		kafkaTemplate.send(topic, key, event);
		return null;
	}

	private String getTopicName(T event) {
		Topic topic = extractTopic(event);
		if (topic != null) {
			return topic.name();
		}
		return event.getClass().getName();
	}

	private Topic extractTopic(T event) {
		return event.getClass().getAnnotation(Topic.class);
	}

	private K extractKey(T event) throws KafkaEntityException {
		for (Field field : event.getClass().getDeclaredFields()) {
			System.out.println("    field  -> " + field.getName());
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
