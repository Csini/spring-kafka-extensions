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
import net.csini.spring.kafka.entity.Student;
import net.csini.spring.kafka.entity.User;

@SpringBootTest(classes = { SpringKafkaEntityTestApplication.class, KafkaEntityConfig.class, ExampleKafkaEntityProducer.class })
public class KafkaEntityProducerTest {

	@Autowired
	private ExampleKafkaEntityProducer productProducer;

	@Test
	public void test_sendEvent() throws KafkaEntityException, InterruptedException, ExecutionException {
		Product event = new Product();
		event.setId("123456");
		Long sendEventTimestamp = productProducer.sendEvent(event);
		System.out.println("sentEventTimestamp: " + sendEventTimestamp);
		Assertions.assertNotNull(sendEventTimestamp);

	}
	
	@Test
	public void test_sendUser() throws KafkaEntityException, InterruptedException, ExecutionException {
		User event = new User("abcdef");
		KafkaEntityException ex = Assertions.assertThrows(KafkaEntityException.class, ()-> productProducer.sendUser(event));
		Assertions.assertEquals("@Key is mandatory in @KafkaEntity", ex.getMessage());
	}
	
	@Test
	public void test_sendStudent() throws KafkaEntityException, InterruptedException, ExecutionException {
		Student event = new Student("hrs123", 23);
		Long sendEventTimestamp = productProducer.sendStudent(event);
		System.out.println("sentStudentTimestamp: " + sendEventTimestamp);
		Assertions.assertNotNull(sendEventTimestamp);

	}
}
