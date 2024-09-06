package net.csini.spring.kafka.observable;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.KafkaFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import io.reactivex.rxjava3.annotations.CheckReturnValue;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.plugins.RxJavaPlugins;
import net.csini.spring.kafka.KafkaEntityException;
import net.csini.spring.kafka.KafkaEntityObservable;
import net.csini.spring.kafka.Key;
import net.csini.spring.kafka.Topic;
import net.csini.spring.kafka.mapping.JsonKeyDeserializer;

public class SimpleKafkaObservableHandler<T, K> extends Observable<T> implements DisposableBean, InitializingBean {

	private static final Logger LOGGER = LoggerFactory.getLogger(SimpleKafkaObservableHandler.class);

	/**
	 * Comma-delimited list of host:port pairs to use for establishing the initial
	 * connections to the Kafka cluster. Applies to all components unless
	 * overridden.
	 */
	@Value(value = "${spring.kafka.bootstrap-servers:localhost:9092}")
	private List<String> bootstrapServers = new ArrayList<>(Collections.singletonList("localhost:9092"));

	private String groupid;

	private Class<T> clazz;

	private Class<K> clazzKey;

//	KafkaConsumer<K, T> kafkaConsumer;

	private AtomicBoolean stopped = new AtomicBoolean(false);

	private AtomicBoolean started = new AtomicBoolean(false);

	private boolean autostart;

	ExecutorService executor;
//	ExecutorService executor = Executors.newSingleThreadScheduledExecutor();

//	ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

	private String beanName;

	SimpleKafkaObservableHandler(KafkaEntityObservable kafkaEntityObservable, String beanName)
			throws KafkaEntityException {
		this.clazz = kafkaEntityObservable.entity();
		this.groupid = /* getTopicName() + "-observer-" + */ beanName;

		this.autostart = kafkaEntityObservable.autostart();

		boolean foundKey = false;

		for (Field field : getClazz().getDeclaredFields()) {
			LOGGER.debug("    field  -> " + field.getName());
			if (field.isAnnotationPresent(Key.class)) {

				Key key = field.getAnnotation(Key.class);

				try {
					field.setAccessible(true);
					this.clazzKey = (Class<K>) field.getType();
				} catch (IllegalArgumentException e) {
					throw new KafkaEntityException(e);
				}
				foundKey = true;
			}
		}
		if (!foundKey) {
			throw new KafkaEntityException("@Key is mandatory in @KafkaEntity");
		}

		subscribers = new AtomicReference<>(EMPTY);

		this.beanName = beanName;

		BasicThreadFactory factory = new BasicThreadFactory.Builder().namingPattern(beanName + "Thread-%d")
				.priority(Thread.MAX_PRIORITY).build();
		executor = Executors.newFixedThreadPool(3, factory);

		if (this.autostart) {
			start();
		}
	}

	public Class<T> getClazz() {
		return this.clazz;
	}

	public Class<K> getClazzKey() {
		return this.clazzKey;
	}

//	@Bean
//	public KafkaConsumer<K, T> consumer() {
//		return this.kafkaConsumer;
//	}

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

//	@Override
//	public void subscribe(@NonNull ObservableEmitter<@NonNull T> emitter) throws Throwable {
//
//	
//	LOGGER.warn("subscribed: " + emitter);
//
//		this.kafkaConsumer.subscribe(List.of(getTopicName()));
//
//		poll().forEach(consumerRecord -> emitter.onNext(consumerRecord.value()));
//
////		emitter.setDisposable(Disposable.fromAction(() -> {
////			kafkaConsumer.unsubscribe();
////			destroy();
////		}));
////		
////		emitter.setCancellable(() -> {
////			kafkaConsumer.unsubscribe();
////			destroy();
////		});
//
//		emitter.onComplete();
//	}

	public void start() {

		if (this.started.get()) {
			LOGGER.warn("already started...");
			return;
		}

		LOGGER.warn("starting " + this.beanName + "...");

		Thread pollingThread = new Thread(() -> {

			Map<String, Object> properties = new HashMap<>();
			properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//			properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
//			properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
			// TODO
			// properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
			// OffsetResetStrategy.LATEST.name().toLowerCase());
			properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupid);

			properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

//			Serde<T> serde = Serdes.serdeFrom(getClazz());

//			properties.put(JsonDeserializer.TRUSTED_PACKAGES, getClazz().getPackageName());
			JsonDeserializer<T> valueDeserializer = new JsonDeserializer<>(getClazz());
			JsonKeyDeserializer<K> keyDeserializer = new JsonKeyDeserializer<>(getClazzKey());
			// TODO
			valueDeserializer.addTrustedPackages(getClazz().getPackageName());
			keyDeserializer.addTrustedPackages(getClazzKey().getPackageName());

			KafkaConsumer<K, T> kafkaConsumer = new KafkaConsumer<K, T>(properties, keyDeserializer, valueDeserializer);

			kafkaConsumer.subscribe(List.of(getTopicName()));

			kafkaConsumer.poll(Duration.ofSeconds(10L));

			kafkaConsumer.seekToEnd(Collections.emptyList());
			kafkaConsumer.commitSync();

			// wait until kafkaConsumer is ready and offset setted
			while (kafkaConsumer.committed(kafkaConsumer.assignment()).isEmpty()) {
				System.out.println("...");
			}
			;

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
			kafkaConsumer.close();
			this.started.set(false);
		});
		pollingThread.setName(beanName + "Thread");
		pollingThread.start();

		LOGGER.warn("waiting the consumer to start in " + beanName + "Thread...");
		while (!this.started.get()) {
			// LOGGER.warn(".");
		}
	}

	public void stop() {
		this.stopped.set(true);
	}

//	private ConsumerRecords<K, T> poll() {
//		LOGGER.warn("POLL-" + groupid + " to " + subscribers.get().length + " subscribers");
//		return this.kafkaConsumer.poll(Duration.ofSeconds(10L));
//	}

//	2024-07-10 | 09:41:35.395 |                                                             pool-2-thread-1 | DEBUG |              o.a.k.clients.NetworkClient | [Consumer clientId=consumer-testgroupid-1, groupId=testgroupid] Sending metadata request MetadataRequestData(topics=[MetadataRequestTopic(topicId=AAAAAAAAAAAAAAAAAAAAAA, name='PRODUCT')], allowAutoTopicCreation=true, includeClusterAuthorizedOperations=false, includeTopicAuthorizedOperations=false) to node localhost:9092 (id: 1 rack: null)
//	2024-07-10 | 09:41:35.395 |                                                             pool-2-thread-1 | DEBUG |              o.a.k.clients.NetworkClient | [Consumer clientId=consumer-testgroupid-1, groupId=testgroupid] Sending METADATA request with header RequestHeader(apiKey=METADATA, apiVersion=12, clientId=consumer-testgroupid-1, correlationId=66, headerVersion=2) and timeout 30000 to node 1: MetadataRequestData(topics=[MetadataRequestTopic(topicId=AAAAAAAAAAAAAAAAAAAAAA, name='PRODUCT')], allowAutoTopicCreation=true, includeClusterAuthorizedOperations=false, includeTopicAuthorizedOperations=false)
//	2024-07-10 | 09:41:35.416 |                                                             pool-2-thread-1 | DEBUG |              o.a.k.clients.NetworkClient | [Consumer clientId=consumer-testgroupid-1, groupId=testgroupid] Received METADATA response from node 1 for request with header RequestHeader(apiKey=METADATA, apiVersion=12, clientId=consumer-testgroupid-1, correlationId=66, headerVersion=2): MetadataResponseData(throttleTimeMs=0, brokers=[MetadataResponseBroker(nodeId=1, host='localhost', port=9092, rack=null)], clusterId='g61plmWdQpKV-8GlKsoGjw', controllerId=1, topics=[MetadataResponseTopic(errorCode=0, name='PRODUCT', topicId=wN29es2YQBWHvG52nyvX2Q, isInternal=false, partitions=[MetadataResponsePartition(errorCode=0, partitionIndex=0, leaderId=1, leaderEpoch=0, replicaNodes=[1], isrNodes=[1], offlineReplicas=[])], topicAuthorizedOperations=-2147483648)], clusterAuthorizedOperations=-2147483648)
//	2024-07-10 | 09:41:35.417 |                                                             pool-2-thread-1 | DEBUG |               o.a.kafka.clients.Metadata | [Consumer clientId=consumer-testgroupid-1, groupId=testgroupid] Updating last seen epoch for partition PRODUCT-0 from 0 to epoch 0 from new metadata
//	2024-07-10 | 09:41:35.418 |                                                             pool-2-thread-1 | DEBUG |               o.a.kafka.clients.Metadata | [Consumer clientId=consumer-testgroupid-1, groupId=testgroupid] Updated cluster metadata updateVersion 34 to MetadataCache{clusterId='g61plmWdQpKV-8GlKsoGjw', nodes={1=localhost:9092 (id: 1 rack: null)}, partitions=[PartitionMetadata(error=NONE, partition=PRODUCT-0, leader=Optional[1], leaderEpoch=Optional[0], replicas=1, isr=1, offlineReplicas=)], controller=localhost:9092 (id: 1 rack: null)}
//	2024-07-10 | 09:41:35.418 |                                                             pool-2-thread-1 | DEBUG |          o.a.k.c.c.i.ConsumerCoordinator | [Consumer clientId=consumer-testgroupid-1, groupId=testgroupid] Sending FindCoordinator request to broker localhost:9092 (id: 1 rack: null)
//	2024-07-10 | 09:41:35.419 |                                                             pool-2-thread-1 | DEBUG |              o.a.k.clients.NetworkClient | [Consumer clientId=consumer-testgroupid-1, groupId=testgroupid] Sending FIND_COORDINATOR request with header RequestHeader(apiKey=FIND_COORDINATOR, apiVersion=4, clientId=consumer-testgroupid-1, correlationId=67, headerVersion=2) and timeout 30000 to node 1: FindCoordinatorRequestData(key='', keyType=0, coordinatorKeys=[testgroupid])
//	2024-07-10 | 09:41:35.431 |                                                             pool-2-thread-1 | DEBUG |              o.a.k.clients.NetworkClient | [Consumer clientId=consumer-testgroupid-1, groupId=testgroupid] Received FIND_COORDINATOR response from node 1 for request with header RequestHeader(apiKey=FIND_COORDINATOR, apiVersion=4, clientId=consumer-testgroupid-1, correlationId=67, headerVersion=2): FindCoordinatorResponseData(throttleTimeMs=0, errorCode=0, errorMessage='', nodeId=0, host='', port=0, coordinators=[Coordinator(key='testgroupid', nodeId=-1, host='', port=-1, errorCode=15, errorMessage='')])
//	2024-07-10 | 09:41:35.431 |                                                             pool-2-thread-1 | DEBUG |          o.a.k.c.c.i.ConsumerCoordinator | [Consumer clientId=consumer-testgroupid-1, groupId=testgroupid] Received FindCoordinator response ClientResponse(receivedTimeMs=1720597295430, latencyMs=11, disconnected=false, requestHeader=RequestHeader(apiKey=FIND_COORDINATOR, apiVersion=4, clientId=consumer-testgroupid-1, correlationId=67, headerVersion=2), responseBody=FindCoordinatorResponseData(throttleTimeMs=0, errorCode=0, errorMessage='', nodeId=0, host='', port=0, coordinators=[Coordinator(key='testgroupid', nodeId=-1, host='', port=-1, errorCode=15, errorMessage='')]))
//	2024-07-10 | 09:41:35.431 |                                                             pool-2-thread-1 | DEBUG |          o.a.k.c.c.i.ConsumerCoordinator | [Consumer clientId=consumer-testgroupid-1, groupId=testgroupid] Group coordinator lookup failed: 
//	2024-07-10 | 09:41:35.431 |                                                             pool-2-thread-1 | DEBUG |          o.a.k.c.c.i.ConsumerCoordinator | [Consumer clientId=consumer-testgroupid-1, groupId=testgroupid] Coordinator discovery failed, refreshing metadata
//	org.apache.kafka.common.errors.CoordinatorNotAvailableException: The coordinator is not available.

	@Override
	public void afterPropertiesSet() throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void destroy() throws Exception {

		LOGGER.warn("*** Starting AdminClient to delete a Consumer Group ***" + this.beanName);

		stop();
		executor.shutdown();

		final Properties properties = new Properties();
		properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "1000");
		properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000");

		try (AdminClient adminClient = AdminClient.create(properties);) {
			String consumerGroupToBeDeleted = this.groupid;
			DeleteConsumerGroupsResult deleteConsumerGroupsResult = adminClient
					.deleteConsumerGroups(Arrays.asList(consumerGroupToBeDeleted));

			KafkaFuture<Void> resultFuture = deleteConsumerGroupsResult.all();
			resultFuture.get();
		}

//	    adminClient.close();		
	}

	/** The terminated indicator for the subscribers array. */
	@SuppressWarnings("rawtypes")
	static final KafkaEntityObservableDisposable[] TERMINATED = new KafkaEntityObservableDisposable[0];
	/** An empty subscribers array to avoid allocating it all the time. */
	@SuppressWarnings("rawtypes")
	static final KafkaEntityObservableDisposable[] EMPTY = new KafkaEntityObservableDisposable[0];

	/** The array of currently subscribed subscribers. */
	final AtomicReference<KafkaEntityObservableDisposable<T, K>[]> subscribers;

	/** The error, write before terminating and read after checking subscribers. */
	Throwable error;

	/**
	 * Constructs a SimpleKafkaObservableHandler.
	 * 
	 * @param <T> the value type
	 * @return the new SimpleKafkaObservableHandler
	 * @throws KafkaEntityException
	 */
	@CheckReturnValue
	@NonNull
	public static <T, K> SimpleKafkaObservableHandler<T, K> create(KafkaEntityObservable kafkaEntityObservable,
			String beanName) throws KafkaEntityException {
		return new SimpleKafkaObservableHandler<T, K>(kafkaEntityObservable, beanName);
	}

	@Override
	protected void subscribeActual(Observer<? super T> t) {
		KafkaEntityObservableDisposable<T, K> ps = new KafkaEntityObservableDisposable<>(t, this);
		t.onSubscribe(ps);
		if (add(ps)) {
			// if cancellation happened while a successful add, the remove() didn't work
			// so we need to do it again
			if (ps.isDisposed()) {
				remove(ps);
			}
		} else {
			Throwable ex = error;
			if (ex != null) {
				t.onError(ex);
			} else {
				t.onComplete();
			}
		}

		if (!this.autostart) {
			start();
		}
	}

	/**
	 * Tries to add the given subscriber to the subscribers array atomically or
	 * returns false if the subject has terminated.
	 * 
	 * @param ps the subscriber to add
	 * @return true if successful, false if the subject has terminated
	 */
	boolean add(KafkaEntityObservableDisposable<T, K> ps) {
		for (;;) {
			KafkaEntityObservableDisposable<T, K>[] a = subscribers.get();
			if (a == TERMINATED) {
				return false;
			}

			int n = a.length;
			@SuppressWarnings("unchecked")
			KafkaEntityObservableDisposable<T, K>[] b = new KafkaEntityObservableDisposable[n + 1];
			System.arraycopy(a, 0, b, 0, n);
			b[n] = ps;

			if (subscribers.compareAndSet(a, b)) {
				return true;
			}
		}
	}

	/**
	 * Atomically removes the given subscriber if it is subscribed to the subject.
	 * 
	 * @param ps the subject to remove
	 */
	@SuppressWarnings("unchecked")
	void remove(KafkaEntityObservableDisposable<T, K> ps) {
		for (;;) {
			KafkaEntityObservableDisposable<T, K>[] a = subscribers.get();
			if (a == TERMINATED || a == EMPTY) {
				return;
			}

			int n = a.length;
			int j = -1;
			for (int i = 0; i < n; i++) {
				if (a[i] == ps) {
					j = i;
					break;
				}
			}

			if (j < 0) {
				return;
			}

			KafkaEntityObservableDisposable<T, K>[] b;

			if (n == 1) {
				b = EMPTY;
			} else {
				b = new KafkaEntityObservableDisposable[n - 1];
				System.arraycopy(a, 0, b, 0, j);
				System.arraycopy(a, j + 1, b, j, n - j - 1);
			}
			if (subscribers.compareAndSet(a, b)) {
				return;
			}
		}
	}

//    @Override
//    public void onSubscribe(Disposable d) {
//        if (subscribers.get() == TERMINATED) {
//            d.dispose();
//        }
//    }
//
//    @Override
//    public void onNext(T t) {
//        ExceptionHelper.nullCheck(t, "onNext called with a null value.");
//        for (KafkaEntityObservableDisposable<T> pd : subscribers.get()) {
//            pd.onNext(t);
//        }
//    }
//
//    @SuppressWarnings("unchecked")
//    @Override
//    public void onError(Throwable t) {
//        ExceptionHelper.nullCheck(t, "onError called with a null Throwable.");
//        if (subscribers.get() == TERMINATED) {
//            RxJavaPlugins.onError(t);
//            return;
//        }
//        error = t;
//
//        for (KafkaEntityObservableDisposable<T> pd : subscribers.getAndSet(TERMINATED)) {
//            pd.onError(t);
//        }
//    }
//
//    @SuppressWarnings("unchecked")
//    @Override
//    public void onComplete() {
//        if (subscribers.get() == TERMINATED) {
//            return;
//        }
//        for (KafkaEntityObservableDisposable<T> pd : subscribers.getAndSet(TERMINATED)) {
//            pd.onComplete();
//        }
//    }
//
//    @Override
//    @CheckReturnValue
//    public boolean hasObservers() {
//        return subscribers.get().length != 0;
//    }
//
//    @Override
//    @Nullable
//    @CheckReturnValue
//    public Throwable getThrowable() {
//        if (subscribers.get() == TERMINATED) {
//            return error;
//        }
//        return null;
//    }
//
//    @Override
//    @CheckReturnValue
//    public boolean hasThrowable() {
//        return subscribers.get() == TERMINATED && error != null;
//    }
//
//    @Override
//    @CheckReturnValue
//    public boolean hasComplete() {
//        return subscribers.get() == TERMINATED && error == null;
//    }

	/**
	 * Wraps the actual subscriber, tracks its requests and makes cancellation to
	 * remove itself from the current subscribers array.
	 *
	 * @param <T> the value type
	 */
	static final class KafkaEntityObservableDisposable<T, K> extends AtomicBoolean implements Disposable {

		private static final long serialVersionUID = 3562861878281475070L;
		/** The actual subscriber. */
		final Observer<? super T> downstream;
		/** The subject state. */
		final SimpleKafkaObservableHandler<T, K> parent;

		/**
		 * Constructs a PublishSubscriber, wraps the actual subscriber and the state.
		 * 
		 * @param actual the actual subscriber
		 * @param parent the parent PublishProcessor
		 */
		KafkaEntityObservableDisposable(Observer<? super T> actual, SimpleKafkaObservableHandler<T, K> parent) {
			this.downstream = actual;
			this.parent = parent;
		}

		public void onNext(T t) {
			if (!get()) {
				downstream.onNext(t);
			}
		}

		public void onError(Throwable t) {
			if (get()) {
				RxJavaPlugins.onError(t);
			} else {
				downstream.onError(t);
			}
		}

		public void onComplete() {
			if (!get()) {
				downstream.onComplete();
			}
		}

		@Override
		public void dispose() {
			if (compareAndSet(false, true)) {
				parent.remove(this);
			}
		}

		@Override
		public boolean isDisposed() {
			return get();
		}
	}

}
