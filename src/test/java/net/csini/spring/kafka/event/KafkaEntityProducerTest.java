package net.csini.spring.kafka.event;

import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import net.csini.spring.kafka.KafkaEntityException;
import net.csini.spring.kafka.SpringKafkaEntityTestApplication;
import net.csini.spring.kafka.config.KafkaEntityConfig;
import net.csini.spring.kafka.entity.Product;

@SpringBootTest(classes = { SpringKafkaEntityTestApplication.class, KafkaEntityConfig.class, ExampleKafkaEntityProducer.class })
public class KafkaEntityProducerTest {

	@Autowired
	private ExampleKafkaEntityProducer productProducer;

	@Test
	public void test_sendEvent() throws KafkaEntityException, InterruptedException, ExecutionException {
		Product event = new Product();
		event.setId("123456");
		Long sendEventTimestamp = productProducer.sendEvent(event);
		System.out.println("sendEventTimestamp: " + sendEventTimestamp);
		Assertions.assertNotNull(sendEventTimestamp);

	}
}
