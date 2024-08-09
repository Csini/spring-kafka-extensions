package net.csini.spring.kafka.producer;

import java.lang.reflect.Field;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonSerializer;

import net.csini.spring.kafka.KafkaEntityException;
import net.csini.spring.kafka.KafkaEntityProducer;
import net.csini.spring.kafka.Key;
import net.csini.spring.kafka.Topic;
import net.csini.spring.kafka.mapping.JsonKeySerializer;

public class SimpleKafkaProducer<T, K>
		/* extends KafkaTemplate<K, T> **/ implements KafkaEntityFutureMaker<T>, DisposableBean, InitializingBean {

	private static final Logger LOGGER = LoggerFactory.getLogger(SimpleKafkaProducer.class);

	/**
	 * Comma-delimited list of host:port pairs to use for establishing the initial
	 * connections to the Kafka cluster. Applies to all components unless
	 * overridden.
	 */
	@Value(value = "${spring.kafka.bootstrap-servers:localhost:9092}")
	private List<String> bootstrapServers = new ArrayList<>(Collections.singletonList("localhost:9092"));

	private String clientid;

	private KafkaProducer<K, T> kafkaProducer;

	private Class<T> clazz;

	private String topic;

	public SimpleKafkaProducer(KafkaEntityProducer kafkaEntityProducer)
			throws InterruptedException, ExecutionException {
		this.clazz = kafkaEntityProducer.entity();
		this.clientid = kafkaEntityProducer.clientid();
		this.topic = getTopicName();

		Map<String, Object> configProps = new HashMap<>();
		configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, JsonKeySerializer.class);
		configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		configProps.put(ProducerConfig.CLIENT_ID_CONFIG, clientid);

		configProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,
				kafkaEntityProducer.entity().getSimpleName() + "-transactional-id");

		configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

//		configProps.put(ProducerConfig.“transaction.state.log.min.isr”, 2);

		JsonSerializer<T> valueSerializer = new JsonSerializer<>();
		JsonKeySerializer<K> keySerializer = new JsonKeySerializer<>();

		this.kafkaProducer = new KafkaProducer<K, T>(configProps, keySerializer, valueSerializer);

//	      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
//	      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
//	      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1

		LOGGER.warn("initTransactions-begin");
		this.kafkaProducer.initTransactions();
		LOGGER.warn("initTransactions-end");
	}

//	@PostConstruct
	@Override
	public void afterPropertiesSet() {
	}

	public Class<T> getClazz() {
		return this.clazz;
	}

//	public CompletableFuture<SendResult<T>> send(T event) {
//		//TODO
//		return null;
//	}

	@Override
	public CompletableFuture<LocalDateTime> call(T event) /* throws KafkaEntityException */ {

		LOGGER.info("sending: " + event);

		CompletableFuture<LocalDateTime> send;

//		try {
		this.kafkaProducer.beginTransaction();
		send = createFutureFromEvent(this.topic, event);
//		} catch (KafkaEntityWarapperException ex) {
//			this.kafkaProducer.abortTransaction();
//			// KafkaEntityException
//			throw ex.getWrapped();
//		}
		if (send.isCompletedExceptionally()) {
			this.kafkaProducer.abortTransaction();
		} else {
			this.kafkaProducer.commitTransaction();
		}
		return send;
	}

	@Override
	public CompletableFuture<Void> call(Stream<T> events) {

		LOGGER.info("sending events");
		String topic = getTopicName();

		this.kafkaProducer.beginTransaction();
		Stream<CompletableFuture<LocalDateTime>> sends;
		sends = events.map(event -> {
			return createFutureFromEvent(topic, event);
		});

		CompletableFuture[] a = (CompletableFuture[]) sends.toArray();
		CompletableFuture<Void> allOf = CompletableFuture.allOf(a);
		if (allOf.isCompletedExceptionally()) {
			this.kafkaProducer.abortTransaction();
		} else {
			this.kafkaProducer.commitTransaction();
		}
		return allOf;
//		return sends;
	}

	private CompletableFuture<LocalDateTime> createFutureFromEvent(String topic, T event) {
		CompletableFuture<LocalDateTime> completableFuture = new CompletableFuture<>();
		K key;
		try {
			key = extractKey(event);
		} catch (KafkaEntityException e) {
//			throw new KafkaEntityWarapperException(e);
			completableFuture.completeExceptionally(e);
			return completableFuture;
		}
		ProducerRecord<K, T> rec = new ProducerRecord<K, T>(topic, key, event);
		Future<RecordMetadata> send = this.kafkaProducer.send(rec);
		try {
			completableFuture.complete(convertToLocalDatetime(send.get()));
		} catch (InterruptedException | ExecutionException e) {
			completableFuture.completeExceptionally(e);
		}

		return completableFuture;
	}

	private LocalDateTime convertToLocalDatetime(RecordMetadata recordMetadata) {

		long epochTimeMillis = recordMetadata.timestamp();
		Instant instant = Instant.ofEpochMilli(epochTimeMillis);

		// Use the system default time zone
		ZoneId zoneId = ZoneId.systemDefault();
		LocalDateTime localDateTime = instant.atZone(zoneId).toLocalDateTime();
		return localDateTime;
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

	// TODO place in constructor to find the field
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

//	@PreDestroy
	@Override
	public void destroy() throws Exception {
		this.kafkaProducer.close();
	}

}
