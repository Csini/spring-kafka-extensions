package net.csini.spring.kafka.subject;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import net.csini.spring.kafka.config.KafkaEntityConfig;
import net.csini.spring.kafka.entity.Product;
import net.csini.spring.kafka.entity.Student;
import net.csini.spring.kafka.entity.User;
import net.csini.spring.kafka.entity.util.TopicUtil;
import net.csini.spring.kafka.exception.KafkaEntityException;

@SpringBootTest(classes = { TopicUtil.class, SpringKafkaEntitySubjectTestConfiguration.class, SpringKafkaEntitySubjectTestApplication.class, KafkaEntityConfig.class,
		OtherKafkaEntitySubjectService.class })
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = { "listeners=PLAINTEXT://localhost:9092", "port=9092",
		"offsets.topic.replication.factor=1", "offset.storage.replication.factor=1",
		"transaction.state.log.replication.factor=1", "transaction.state.log.min.isr=1" })
public class KafkaEntitySubjectTest {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaEntitySubjectTest.class);

	@Autowired
	private OtherKafkaEntitySubjectService obs;

	@Autowired
	private KafkaEntityConfig kafkaEntityConfig;

	@Test
	public void test_sendEvent() throws KafkaEntityException, InterruptedException, ExecutionException {
		
		Product event = new Product();
		event.setId("123456");
		RecordMetadata sendEventMetadata = obs.sendEvent(event);
		LOGGER.info("sendEventMetadata: " + sendEventMetadata.offset());
		Assertions.assertNotNull(sendEventMetadata);

	}

	@Test
	public void test_sendUser() throws KafkaEntityException, InterruptedException, ExecutionException {
		
		User event = new User("abcdef");
		Assertions.assertThrows(NullPointerException.class, () -> obs.sendUser(event));

		KafkaEntityException ex = Assertions.assertThrows(KafkaEntityException.class,
				() -> kafkaEntityConfig.throwFirstError());
		Assertions.assertEquals("net.csini.spring.kafka.subject.OtherKafkaEntitySubjectService#userSubject: net.csini.spring.kafka.entity.User @KafkaEntityKey is mandatory in @KafkaEntity", ex.getMessage());
	}

	@Test
	public void test_sendStudent() throws KafkaEntityException, InterruptedException, ExecutionException {
		
		Student event = new Student("hrs123", 23);
		RecordMetadata sendStudentMetadata = obs.sendStudent(event);
		LOGGER.info("sendStudentMetadata: " + sendStudentMetadata.offset());
		Assertions.assertNotNull(sendStudentMetadata);

	}

}
