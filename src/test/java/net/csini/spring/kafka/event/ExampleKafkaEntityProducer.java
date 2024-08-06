package net.csini.spring.kafka.event;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;

import net.csini.spring.kafka.KafkaEntityException;
import net.csini.spring.kafka.KafkaEntityProducer;
import net.csini.spring.kafka.entity.Product;
import net.csini.spring.kafka.entity.Student;
import net.csini.spring.kafka.entity.User;
import net.csini.spring.kafka.producer.KafkaEntityFutureMaker;

@Service
public class ExampleKafkaEntityProducer {

	@KafkaEntityProducer(entity = Product.class, clientid = "productTestExampleKafkaEntityProducer")
	private KafkaEntityFutureMaker<Product> productProducer;

	@KafkaEntityProducer(entity = User.class, clientid = "userExampleKafkaEntityProducer")
	private KafkaEntityFutureMaker<User> userProducer;

	@KafkaEntityProducer(entity = Student.class, clientid = "studentExampleKafkaEntityProducer")
	private KafkaEntityFutureMaker<Student> studentProducer;

	public LocalDateTime sendEvent(Product p) throws KafkaEntityException, InterruptedException, ExecutionException {
		return productProducer.call(p).get();
	}

	public LocalDateTime sendUser(User u) throws InterruptedException, ExecutionException {
		return userProducer.call(u).get();
	}

	public LocalDateTime sendStudent(Student s) throws InterruptedException, ExecutionException {
		return studentProducer.call(s).get();
	}

}
