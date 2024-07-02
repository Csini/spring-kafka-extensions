package net.csini.spring.kafka.event;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import net.csini.spring.kafka.SpringKafkaEntityTestApplication;
import net.csini.spring.kafka.config.KafkaEntityConfig;
import net.csini.spring.kafka.entity.Product;

@SpringBootTest(classes = { SpringKafkaEntityTestApplication.class, KafkaEntityConfig.class, ExampleKafkaEntityProducer.class })
public class KafkaEntityProducerTest {

	@Autowired
	private ExampleKafkaEntityProducer productProducer;

	@Test
	public void test_sendEvent() {
		Product event = new Product();
		event.setId("123456");
		Assertions.assertNotNull(productProducer.sendEvent(event));

	}
}
