package net.csini.spring.kafka.event;

import java.util.concurrent.ExecutionException;

import org.springframework.stereotype.Service;

import net.csini.spring.kafka.KafkaEntityException;
import net.csini.spring.kafka.KafkaEntityProducer;
import net.csini.spring.kafka.entity.Product;
import net.csini.spring.kafka.entity.Student;
import net.csini.spring.kafka.entity.User;
import net.csini.spring.kafka.producer.SimpleKafkaProducer;

@Service
public class ExampleKafkaEntityProducer {

	@KafkaEntityProducer(entity = Product.class, clientid = "testExampleKafkaEntityProducer")
	private SimpleKafkaProducer<Product> productProducer;
	
	
	@KafkaEntityProducer(entity = User.class, clientid = "userExampleKafkaEntityProducer")
	private SimpleKafkaProducer<User> userProducer;
	
	@KafkaEntityProducer(entity = Student.class, clientid = "studentExampleKafkaEntityProducer")
	private SimpleKafkaProducer<Student> studentProducer;
	
	public Long sendEvent(Product p) throws KafkaEntityException, InterruptedException, ExecutionException {
		return productProducer.send(p).get();
	}
	
	public Long sendUser(User u) throws KafkaEntityException, InterruptedException, ExecutionException {
		return userProducer.send(u).get();
	}
	
	public Long sendStudent(Student s) throws KafkaEntityException, InterruptedException, ExecutionException {
		return studentProducer.send(s).get();
	}
	
}
