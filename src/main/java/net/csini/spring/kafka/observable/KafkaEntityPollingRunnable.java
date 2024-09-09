package net.csini.spring.kafka.observable;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import net.csini.spring.kafka.Topic;
import net.csini.spring.kafka.mapping.JsonKeyDeserializer;
import net.csini.spring.kafka.observable.SimpleKafkaObservable.KafkaEntityObservableDisposable;

public class KafkaEntityPollingRunnable<T, K> implements Runnable {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaEntityPollingRunnable.class);

	private final String groupid;

	private final Class<T> clazz;

	private final Class<K> clazzKey;

	private final AtomicBoolean stopped = new AtomicBoolean(false);

	private final AtomicBoolean started = new AtomicBoolean(false);

	/** The array of currently subscribed subscribers. */
	private final AtomicReference<KafkaEntityObservableDisposable<T, K>[]> subscribers;

	private final List<String> bootstrapServers;
	
	private final String beanName;

	public KafkaEntityPollingRunnable(String groupid, Class<T> clazz, Class<K> clazzKey, AtomicReference<KafkaEntityObservableDisposable<T, K>[]> subscribers,
			List<String> bootstrapServers, String beanName) {
		super();
		this.groupid = groupid;
		this.clazz = clazz;
		this.clazzKey = clazzKey;
		this.subscribers = subscribers;
		this.bootstrapServers = bootstrapServers;
		this.beanName = beanName;
	}

	@Override
	public void run() {

		Map<String, Object> properties = new HashMap<>();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
//		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
		// TODO
		// properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
		// OffsetResetStrategy.LATEST.name().toLowerCase());
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupid);

		properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

//		Serde<T> serde = Serdes.serdeFrom(getClazz());

//		properties.put(JsonDeserializer.TRUSTED_PACKAGES, getClazz().getPackageName());
		JsonDeserializer<T> valueDeserializer = new JsonDeserializer<>(getClazz());
		JsonKeyDeserializer<K> keyDeserializer = new JsonKeyDeserializer<>(getClazzKey());
		// TODO
		valueDeserializer.addTrustedPackages(getClazz().getPackageName());
		keyDeserializer.addTrustedPackages(getClazzKey().getPackageName());

		try (KafkaConsumer<K, T> kafkaConsumer = new KafkaConsumer<K, T>(properties, keyDeserializer,
				valueDeserializer);) {

			kafkaConsumer.subscribe(List.of(getTopicName()));

			kafkaConsumer.poll(Duration.ofSeconds(10L));

			kafkaConsumer.seekToEnd(Collections.emptyList());
			kafkaConsumer.commitSync();

			// wait until kafkaConsumer is ready and offset setted

			LocalDateTime then = LocalDateTime.now();
			while (kafkaConsumer.committed(kafkaConsumer.assignment()).isEmpty()) {
				System.out.print("...");
				if (ChronoUnit.SECONDS.between(then, LocalDateTime.now()) >= 20) {
//				break;
					// TODO
					throw new RuntimeException("KafkaConsumer is not ready.");
				}
			}

			LOGGER.warn("started " + this.beanName + "...");
			while (!stopped.get()) {
				this.started.set(true);
				
				if (subscribers.get().length > 0) {
					LOGGER.warn("POLL-" + groupid + " to " + subscribers.get().length + " subscribers");
					ConsumerRecords<K, T> poll = kafkaConsumer.poll(Duration.ofSeconds(10L));

					LOGGER.warn("count: " + poll.count());

					poll.forEach(r -> {
						// TODO
						LOGGER.warn("polled:" + r);

						for (KafkaEntityObservableDisposable<T, K> pd : subscribers.get()) {
							pd.onNext(r.value());
						}
					});
					kafkaConsumer.commitSync();
				}
			}
			kafkaConsumer.unsubscribe();
//		kafkaConsumer.close();
			this.started.set(false);
		}
	}

	public Class<T> getClazz() {
		return this.clazz;
	}

	public Class<K> getClazzKey() {
		return this.clazzKey;
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

	public AtomicBoolean getStopped() {
		return stopped;
	}

	public AtomicBoolean getStarted() {
		return started;
	}
	
}