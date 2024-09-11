package net.csini.spring.kafka.subject;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Service;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Single;
import net.csini.spring.kafka.KafkaEntitySubject;
import net.csini.spring.kafka.entity.Product;
import net.csini.spring.kafka.entity.Student;
import net.csini.spring.kafka.entity.User;

@Service
public class OtherKafkaEntitySubjectService {

	@KafkaEntitySubject(entity = Product.class)
	private KafkaSubject<Product> productSubject;

	@KafkaEntitySubject(entity = User.class)
	private KafkaSubject<User> userSubject;

	@KafkaEntitySubject(entity = Student.class)
	private KafkaSubject<Student> studentSubject;

	public RecordMetadata sendEvent(Product p) {
		List<RecordMetadata> ret = new ArrayList<>();
		productSubject.subscribe(r -> {
			ret.add(r);
		});
		Single.just(p).toObservable().subscribe(productSubject);
		return ret.get(0);
	}

	public RecordMetadata sendUser(User u) {
		List<RecordMetadata> ret = new ArrayList<>();
		userSubject.subscribe(r -> {
			ret.add(r);
		});
		Observable.just(u).subscribe(userSubject);
		return ret.get(0);
	}

	public RecordMetadata sendStudent(Student s) {
		List<RecordMetadata> ret = new ArrayList<>();
		studentSubject.subscribe(r -> {
			ret.add(r);
		});
		Observable.fromArray(s).subscribe(studentSubject);
		return ret.get(0);
	}
	

}
