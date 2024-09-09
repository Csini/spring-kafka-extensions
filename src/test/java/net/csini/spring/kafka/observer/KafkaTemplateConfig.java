package net.csini.spring.kafka.observer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import net.csini.spring.kafka.entity.City;
import net.csini.spring.kafka.mapping.JsonKeyDeserializer;
import net.csini.spring.kafka.mapping.JsonKeySerializer;

@Configuration
public class KafkaTemplateConfig {

	/**
	 * Comma-delimited list of host:port pairs to use for establishing the initial
	 * connections to the Kafka cluster. Applies to all components unless
	 * overridden.
	 */
	@Value(value = "${spring.kafka.bootstrap-servers:localhost:9092}")
	private List<String> bootstrapServers = new ArrayList<>(Collections.singletonList("localhost:9092"));

	public ConsumerFactory<String, City> consumerFactory() {
		Map<String, Object> config = new HashMap<>();
		config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		config.put(ConsumerConfig.GROUP_ID_CONFIG, "teeest");
//		config.put(JsonDeserializer.TRUSTED_PACKAGES, City.class.getPackageName());
//		config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.LATEST.name().toLowerCase());

		JsonDeserializer<City> valueDeserializer = new JsonDeserializer<City>();
		valueDeserializer.addTrustedPackages(City.class.getPackageName());
		return new DefaultKafkaConsumerFactory<>(config, new JsonKeyDeserializer<String>(String.class),
				valueDeserializer);
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, City> kafkaListenerContainerFactory() {

		ConcurrentKafkaListenerContainerFactory<String, City> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory());
		return factory;
	}

//	@Bean
//	public KafkaTemplate<String, City> kafkaTemplate() {
//		KafkaTemplate<String, City> kafkaTemplate = new KafkaTemplate<>(producerFactory());
//		kafkaTemplate.setConsumerFactory(consumerFactory());
//		return kafkaTemplate;
//	}
}