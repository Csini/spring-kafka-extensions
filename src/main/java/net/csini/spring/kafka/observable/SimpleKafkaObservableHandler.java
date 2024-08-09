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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.annotations.Nullable;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.ObservableEmitter;
import io.reactivex.rxjava3.core.ObservableOnSubscribe;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.functions.Cancellable;
import io.reactivex.rxjava3.observables.ConnectableObservable;
import net.csini.spring.kafka.KafkaEntityException;
import net.csini.spring.kafka.KafkaEntityObservable;
import net.csini.spring.kafka.Key;
import net.csini.spring.kafka.Topic;
import net.csini.spring.kafka.mapping.JsonKeyDeserializer;

public class SimpleKafkaObservableHandler<T, K> implements ObservableOnSubscribe<T>, DisposableBean, InitializingBean {

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

	KafkaConsumer<K, T> kafkaConsumer;

//	ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

	public SimpleKafkaObservableHandler(KafkaEntityObservable kafkaEntityObservable) throws KafkaEntityException {
		this.clazz = kafkaEntityObservable.entity();
		this.groupid = getTopicName() + "-observer";

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
	}

	public Class<T> getClazz() {
		return this.clazz;
	}

	public Class<K> getClazzKey() {
		return this.clazzKey;
	}

	@Bean
	public KafkaConsumer<K, T> consumer() {
		return this.kafkaConsumer;
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

	@Override
	public void subscribe(@NonNull ObservableEmitter<@NonNull T> emitter) throws Throwable {

		Map<String, Object> properties = new HashMap<>();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
//		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
		// TODO
//		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.LATEST.name().toLowerCase());
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupid);

//		Serde<T> serde = Serdes.serdeFrom(getClazz());

//		properties.put(JsonDeserializer.TRUSTED_PACKAGES, getClazz().getPackageName());
		JsonDeserializer<T> valueDeserializer = new JsonDeserializer<>(getClazz());
		JsonKeyDeserializer<K> keyDeserializer = new JsonKeyDeserializer<>(getClazzKey());
		// TODO
		valueDeserializer.addTrustedPackages(getClazz().getPackageName());
		keyDeserializer.addTrustedPackages(getClazzKey().getPackageName());

		this.kafkaConsumer = new KafkaConsumer<K, T>(properties, keyDeserializer, valueDeserializer);

		kafkaConsumer.seekToEnd(Collections.emptyList());
		kafkaConsumer.commitSync();

		this.kafkaConsumer.subscribe(List.of(getTopicName()));

		poll().forEach(consumerRecord -> emitter.onNext(consumerRecord.value()));

		emitter.setDisposable(Disposable.fromAction(() -> {
			kafkaConsumer.unsubscribe();
			destroy();
		}));
		
		emitter.setCancellable(() -> {
			kafkaConsumer.unsubscribe();
			destroy();
		});
	}

	private ConsumerRecords<K, T> poll() {
		LOGGER.warn("POLL");
		return this.kafkaConsumer.poll(Duration.ofSeconds(10L));
	}

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

		LOGGER.warn("*** Starting AdminClient to delete a Consumer Group ***");

		kafkaConsumer.unsubscribe();
		kafkaConsumer.close();
		
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

}
