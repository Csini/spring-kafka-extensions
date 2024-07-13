package net.csini.spring.kafka.event;

import org.junit.jupiter.api.Assertions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import net.csini.spring.kafka.SpringKafkaEntityTestApplication;
import net.csini.spring.kafka.config.KafkaEntityConfig;

@SpringBootTest(classes = {SpringKafkaEntityTestApplication.class, KafkaEntityConfig.class, KafkaEntityListenersSpy.class})
public class KafkaEntityListenerTest {

	@Autowired
	private KafkaEntityListenersSpy kafkaEntityListenersSpy;
	
	public void test_product_listener() {
		
		
		//TODO
		
		Assertions.assertEquals(5, kafkaEntityListenersSpy.getSum());
		Assertions.assertEquals(2,kafkaEntityListenersSpy.getInvokeCount());
	}
}
