package net.csini.spring.kafka.observer;

import java.lang.reflect.Field;
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
import net.csini.spring.kafka.KafkaEntityKey;
import net.csini.spring.kafka.KafkaEntityObserver;
import net.csini.spring.kafka.exception.KafkaEntityException;
import net.csini.spring.kafka.mapping.JsonKeySerializer;
import net.csini.spring.kafka.util.KafkaEntityUtil;

/**
 * implementation class for @KafkaEntityObserver
 * 
 * @param <T> class of the entity, representing messages
 * @param <K> the key from the entity
 * 
 * @author Csini
 */
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

	private boolean transactional;

	/**
	 * Constructs a SimpleKafkaObserver.
	 * 
	 * @param <T>                 the value type
	 * @param <K>                 the key type
	 * @param kafkaEntityObserver annotation
	 * @param beanName            name of the Kafka Entity Bean
	 * 
	 * @return the new SimpleKafkaObserver
	 * @throws KafkaEntityException problem at creation
	 */
	@CheckReturnValue
	@NonNull
	public static <T, K> SimpleKafkaEntityObserver<T, K> create(KafkaEntityObserver kafkaEntityObserver,
			String beanName) throws KafkaEntityException {
		return new SimpleKafkaEntityObserver<>(kafkaEntityObserver, beanName);
	}

	SimpleKafkaEntityObserver(KafkaEntityObserver kafkaEntityObserver, String beanName) throws KafkaEntityException {
		this.clazz = kafkaEntityObserver.entity();
		this.clientid = beanName;
		this.topic = KafkaEntityUtil.getTopicName(this.clazz);
		this.transactional = kafkaEntityObserver.transactional();

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

		if (transactional) {
			configProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, beanName + "-transactional-id");
			configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
//		configProps.put(ProducerConfig.“transaction.state.log.min.isr”, 2);
		}

		JsonSerializer<T> valueSerializer = new JsonSerializer<>();
		JsonKeySerializer<K> keySerializer = new JsonKeySerializer<>();

		this.kafkaProducer = new KafkaProducer<K, T>(configProps, keySerializer, valueSerializer);

		if (transactional) {
//	      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
//	      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
//	      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1

			LOGGER.info("initTransactions-begin " + beanName);
			this.kafkaProducer.initTransactions();
			LOGGER.info("initTransactions-end " + beanName);
		}
	}

	private K extractKey(T event) throws IllegalArgumentException, IllegalAccessException {
		return (K) this.keyField.get(event);
	}

	@Override
	public void onSubscribe(Disposable d) {
		if (LOGGER.isTraceEnabled()) {
			LOGGER.trace("sending events");
		}

		if (transactional) {
			this.kafkaProducer.beginTransaction();
		}
	}

	@Override
	public void onNext(T t) {
		ExceptionHelper.nullCheck(t, "onNext called with a null value.");
		CompletableFuture<RecordMetadata> completableFuture = new CompletableFuture<>();
		K key;
		try {
			key = extractKey(t);

			ProducerRecord<K, T> rec = new ProducerRecord<K, T>(topic, key, t);
			Future<RecordMetadata> send = this.kafkaProducer.send(rec);
			completableFuture.complete(send.get());
		} catch (InterruptedException | ExecutionException e) {
			completableFuture.completeExceptionally(e);
		} catch (IllegalAccessException e) {
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
		if (transactional) {
			this.kafkaProducer.abortTransaction();
		}
		return;
	}

	@Override
	public void onComplete() {
		if (transactional) {
			this.kafkaProducer.commitTransaction();
		}
	}

	@Override
	public void afterPropertiesSet() throws Exception {

	}

	@Override
	public void destroy() throws Exception {
		this.kafkaProducer.close();
	}
}
