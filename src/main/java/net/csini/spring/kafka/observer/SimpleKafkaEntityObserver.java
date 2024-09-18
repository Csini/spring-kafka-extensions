package net.csini.spring.kafka.observer;

import java.lang.reflect.Field;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

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

import io.reactivex.rxjava3.annotations.CheckReturnValue;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.internal.util.ExceptionHelper;
import io.reactivex.rxjava3.plugins.RxJavaPlugins;
import net.csini.spring.kafka.KafkaEntityObserver;
import net.csini.spring.kafka.KafkaEntityKey;
import net.csini.spring.kafka.exception.KafkaEntityException;
import net.csini.spring.kafka.mapping.JsonKeySerializer;
import net.csini.spring.kafka.util.KafkaEntityUtil;

public final class SimpleKafkaEntityObserver<T, K> implements Observer<T>, DisposableBean, InitializingBean {

	private static final Logger LOGGER = LoggerFactory.getLogger(SimpleKafkaEntityObserver.class);

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

	/** The error, write before terminating and read after checking subscribers. */
	Throwable error;

	private Field keyField;

	/**
	 * Constructs a SimpleKafkaObserver.
	 * 
	 * @param <T> the value type
	 * @return the new SimpleKafkaObserver
	 * @throws KafkaEntityException
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	@CheckReturnValue
	@NonNull
	public static <T, K> SimpleKafkaEntityObserver<T, K> create(KafkaEntityObserver kafkaEntityObserver,
			String beanName) throws InterruptedException, ExecutionException, KafkaEntityException {
		return new SimpleKafkaEntityObserver<>(kafkaEntityObserver, beanName);
	}

	/**
	 * Constructs a SimpleKafkaObserver.
	 * 
	 * @throws KafkaEntityException
	 * 
	 */
	SimpleKafkaEntityObserver(KafkaEntityObserver kafkaEntityObserver, String beanName)
			throws InterruptedException, ExecutionException, KafkaEntityException {
		this.clazz = kafkaEntityObserver.entity();
		this.clientid = beanName;
		this.topic = KafkaEntityUtil.getTopicName(this.clazz);

		// presents of @KafkaEntityKey is checked in KafkaEntityConfig
		for (Field field : this.clazz.getDeclaredFields()) {
			LOGGER.debug("    field  -> " + field.getName());
			if (field.isAnnotationPresent(KafkaEntityKey.class)) {
				field.setAccessible(true);
				this.keyField = field;
				break;
			}
		}

		Map<String, Object> configProps = new HashMap<>();
		configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, JsonKeySerializer.class);
		configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		configProps.put(ProducerConfig.CLIENT_ID_CONFIG, clientid);

		configProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, beanName + "-transactional-id");

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

	public Class<T> getClazz() {
		return this.clazz;
	}

//	public Class<K> getClazzKey() {
//		return this.clazzKey;
//	}


	private K extractKey(T event) throws IllegalArgumentException, IllegalAccessException {
		return (K) this.keyField.get(event);
	}

	private LocalDateTime convertToLocalDatetime(RecordMetadata recordMetadata) {

		long epochTimeMillis = recordMetadata.timestamp();
		Instant instant = Instant.ofEpochMilli(epochTimeMillis);

		// Use the system default time zone
		ZoneId zoneId = ZoneId.systemDefault();
		LocalDateTime localDateTime = instant.atZone(zoneId).toLocalDateTime();
		return localDateTime;
	}

	@Override
	public void onSubscribe(Disposable d) {
		LOGGER.info("sending events");
//		String topic = KafkaEntityUtil.getTopicName(getClazz());

		this.kafkaProducer.beginTransaction();
	}

	@Override
	public void onNext(T t) {
		ExceptionHelper.nullCheck(t, "onNext called with a null value.");
		CompletableFuture<LocalDateTime> completableFuture = new CompletableFuture<>();
		K key;
		try {
			key = extractKey(t);

			ProducerRecord<K, T> rec = new ProducerRecord<K, T>(topic, key, t);
			Future<RecordMetadata> send = this.kafkaProducer.send(rec);
			completableFuture.complete(convertToLocalDatetime(send.get()));
		} catch (InterruptedException | ExecutionException e) {
			completableFuture.completeExceptionally(e);
		} catch (IllegalAccessException e) {
//			throw new KafkaEntityWarapperException(e);
			completableFuture.completeExceptionally(e);
		}

		try {
			completableFuture.get();
		} catch (InterruptedException | ExecutionException e) {
			onError(e);
		}
	}

	@Override
	public void onError(Throwable t) {
		ExceptionHelper.nullCheck(t, "onError called with a null Throwable.");
		RxJavaPlugins.onError(t);
		this.kafkaProducer.abortTransaction();
		return;
	}

	@Override
	public void onComplete() {
		this.kafkaProducer.commitTransaction();
	}

	@Override
	public void afterPropertiesSet() throws Exception {

	}

	@Override
	public void destroy() throws Exception {
		this.kafkaProducer.close();
	}
}
