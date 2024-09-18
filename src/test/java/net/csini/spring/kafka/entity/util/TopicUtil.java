package net.csini.spring.kafka.entity.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import net.csini.spring.kafka.exception.KafkaEntityException;

@Component
public class TopicUtil {

	private static final Logger LOGGER = LoggerFactory.getLogger(TopicUtil.class);

	@Value(value = "${spring.kafka.bootstrap-servers:localhost:9092}")
	private List<String> bootstrapServers = new ArrayList<>(Collections.singletonList("localhost:9092"));

	public void createTopic(String topicName) throws InterruptedException, ExecutionException, KafkaEntityException {
		Map<String, Object> conf = new HashMap<>();
		conf.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		conf.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
		try (AdminClient admin = AdminClient.create(conf);) {

			ListTopicsResult listTopics = admin.listTopics();
			Set<String> names = listTopics.names().get();
			if(LOGGER.isDebugEnabled()) {
				LOGGER.debug("names: " + names);
			}
			boolean contains = names.contains(topicName);
			if (!contains) {

				Map<String, String> configs = new HashMap<String, String>();
				int partitions = 1;
				Short replication = 1;
				NewTopic newTopic = new NewTopic(topicName, partitions, replication).configs(configs);
				LOGGER.info("creating Topic " + newTopic);
				CreateTopicsResult topicsResult = admin.createTopics(List.of(newTopic));

				topicsResult.all().get();
//				
//				admin.describeTopics(List.of(topicName)).allTopicNames().get().get(topicName).partitions()
//						.forEach(p -> {
//							p.replicas().size();
//						});

//				LOGGER.warn("waiting 10_0000");
//				Thread.sleep(10_000);
			}
		}
	}
}
