package net.csini.spring.kafka.observable;

import org.springframework.stereotype.Service;

import io.reactivex.rxjava3.core.Observable;
import lombok.Getter;
import net.csini.spring.kafka.KafkaEntityObservable;
import net.csini.spring.kafka.entity.Product;

@Service
@Getter
public class ExampleKafkaEntityObserver {

	@KafkaEntityObservable(entity = Product.class, groupid = "testgroupid")
	private Observable<Product> productObservable;

}
	