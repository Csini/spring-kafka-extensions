package net.csini.spring.kafka.event;

import java.util.stream.Stream;

import org.springframework.boot.test.context.SpringBootTest;

import net.csini.spring.kafka.KafkaEntityListener;
import net.csini.spring.kafka.SpringKafkaEntityTestApplication;
import net.csini.spring.kafka.config.KafkaEntityConfig;
import net.csini.spring.kafka.entity.Product;

@SpringBootTest(classes = {SpringKafkaEntityTestApplication.class, KafkaEntityConfig.class})
public class KafkaEntityListenerTest {

	@KafkaEntityListener(entity = Product.class, batchSize=10)
	public void test_product_listener(Stream<Product> events) {
		System.out.println("Received message: " + events);

	}
}
