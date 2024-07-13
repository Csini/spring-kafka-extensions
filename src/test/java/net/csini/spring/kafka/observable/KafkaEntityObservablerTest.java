package net.csini.spring.kafka.observable;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;

import net.csini.spring.kafka.SpringKafkaEntityTestApplication;
import net.csini.spring.kafka.config.KafkaEntityConfig;
import net.csini.spring.kafka.entity.Product;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SpringBootTest(classes = { SpringKafkaEntityTestApplication.class, KafkaEntityConfig.class,
		ExampleKafkaEntityObserver.class, KafkaProducerConfig.class })
public class KafkaEntityObservablerTest {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaEntityObservablerTest.class);

	@Autowired
	private ExampleKafkaEntityObserver observer;

	@Autowired
	private KafkaTemplate<String, Product> kafkaProducer;

	private String TOPIC = "PRODUCT";

	@Test
	public void test_sendEvent() throws Exception {
		List<Product> eventList = new ArrayList<>();


		observer.getProductObservable().subscribe(r -> {
			LOGGER.info("received: " + r);
			eventList.add(r);
		});

		
		Thread.sleep(5000);

		//send events
		publishMessages();

		System.out.println("eventList: " + eventList);
		Assertions.assertEquals(2, eventList.size());

	}

	void publishMessages() throws Exception {

		LOGGER.debug("publishMessages");

		Product p1 = new Product();
		p1.setId("p1id");
		ProducerRecord<String, Product> p1Record = new ProducerRecord<>(TOPIC, p1.getId(), p1);
		kafkaProducer.send(p1Record).get();

		Product p2 = new Product();
		p2.setId("p2id");
		ProducerRecord<String, Product> p2Record = new ProducerRecord<>(TOPIC, p2.getId(), p2);
		kafkaProducer.send(p2Record).get();
	}

}
