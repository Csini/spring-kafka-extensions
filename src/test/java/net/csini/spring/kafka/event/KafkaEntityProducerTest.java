package net.csini.spring.kafka.event;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import net.csini.spring.kafka.KafkaEntityException;
import net.csini.spring.kafka.config.KafkaEntityConfig;
import net.csini.spring.kafka.entity.Product;
import net.csini.spring.kafka.entity.Student;
import net.csini.spring.kafka.entity.User;

@SpringBootTest(classes = { SpringKafkaEntityProducerTestApplication.class, KafkaEntityConfig.class,
		ExampleKafkaEntityProducer.class })
public class KafkaEntityProducerTest {

	@Autowired
	private ExampleKafkaEntityProducer productProducer;

	@Test
	public void test_sendEvent() throws KafkaEntityException, InterruptedException, ExecutionException {
		Product event = new Product();
		event.setId("123456");
		LocalDateTime sendEventTimestamp = productProducer.sendEvent(event);
		System.out.println("sentEventTimestamp: " + sendEventTimestamp);
		Assertions.assertNotNull(sendEventTimestamp);

	}

	@Test
	public void test_sendUser() throws KafkaEntityException, InterruptedException, ExecutionException {
		User event = new User("abcdef");
		ExecutionException ex = Assertions.assertThrows(ExecutionException.class,
				() -> productProducer.sendUser(event));
		Assertions.assertEquals(KafkaEntityException.class, ex.getCause().getClass());
		Assertions.assertEquals("@Key is mandatory in @KafkaEntity", ex.getCause().getMessage());
	}

	@Test
	public void test_sendStudent() throws KafkaEntityException, InterruptedException, ExecutionException {
		Student event = new Student("hrs123", 23);
		LocalDateTime sendEventTimestamp = productProducer.sendStudent(event);
		System.out.println("sentStudentTimestamp: " + sendEventTimestamp);
		Assertions.assertNotNull(sendEventTimestamp);

	}
}
