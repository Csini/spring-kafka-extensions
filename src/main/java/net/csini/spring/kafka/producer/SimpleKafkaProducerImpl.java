package net.csini.spring.kafka.producer;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.serializer.JsonSerializer;

import net.csini.spring.kafka.KafkaEntityException;
import net.csini.spring.kafka.KafkaEntityProducer;
import net.csini.spring.kafka.Key;
import net.csini.spring.kafka.Topic;

public class SimpleKafkaProducerImpl<T, K> /* extends KafkaTemplate<K, T> **/ implements SimpleKafkaProducer<T> {

//	@Autowired
//	KafkaTemplate<K, T> kafkaTemplate;

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

//	private void applyKafkaConnectionDetailsForProducer(Map<String, Object> properties,
//			KafkaConnectionDetails connectionDetails) {
//		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connectionDetails.getProducerBootstrapServers());
//		if (!(connectionDetails instanceof PropertiesKafkaConnectionDetails)) {
//			properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");
//		}
//	}
//	
//	@Bean
//	public DefaultKafkaProducerFactory<?, ?> kafkaProducerFactory(KafkaConnectionDetails connectionDetails,
//			ObjectProvider<DefaultKafkaProducerFactoryCustomizer> customizers) {
//		Map<String, Object> properties = this.properties.buildProducerProperties();
//		applyKafkaConnectionDetailsForProducer(properties, connectionDetails);
//		DefaultKafkaProducerFactory<?, ?> factory = new DefaultKafkaProducerFactory<>(properties);
//		String transactionIdPrefix = this.properties.getProducer().getTransactionIdPrefix();
//		if (transactionIdPrefix != null) {
//			factory.setTransactionIdPrefix(transactionIdPrefix);
//		}
//		customizers.orderedStream().forEach((customizer) -> customizer.customize(factory));
//		return factory;
//	}
//	
//	@Bean
//	public KafkaTemplate<?, ?> kafkaTemplate(ProducerFactory<Object, Object> kafkaProducerFactory,
//			ProducerListener<Object, Object> kafkaProducerListener,
//			ObjectProvider<RecordMessageConverter> messageConverter) {
//		PropertyMapper map = PropertyMapper.get().alwaysApplyingWhenNonNull();
//		KafkaTemplate<Object, Object> kafkaTemplate = new KafkaTemplate<>(kafkaProducerFactory);
//		messageConverter.ifUnique(kafkaTemplate::setMessageConverter);
//		map.from(kafkaProducerListener).to(kafkaTemplate::setProducerListener);
//		map.from(this.properties.getTemplate().getDefaultTopic()).to(kafkaTemplate::setDefaultTopic);
//		map.from(this.properties.getTemplate().getTransactionIdPrefix()).to(kafkaTemplate::setTransactionIdPrefix);
//		return kafkaTemplate;
//	}
	// topic

	// key

	private Class<T> clazz;

	public SimpleKafkaProducerImpl(KafkaEntityProducer kafkaEntityProducer) {
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

	public CompletableFuture<Long> send(T event) throws KafkaEntityException {

		System.out.println("sending: " + event);
		String topic = getTopicName(event);

		K key = extractKey(event);

		return simpleKafkaTemplate().send(topic, key, event) .thenApply(sendresult -> sendresult.getRecordMetadata().timestamp());
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
