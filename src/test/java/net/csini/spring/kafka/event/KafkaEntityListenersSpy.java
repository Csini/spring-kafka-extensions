package net.csini.spring.kafka.event;

import java.util.stream.Stream;

import org.springframework.stereotype.Component;

import lombok.Getter;
import net.csini.spring.kafka.KafkaEntityListener;
import net.csini.spring.kafka.entity.Product;

@Component
@Getter
public class KafkaEntityListenersSpy {
	
	private int sum = 0;
	
	private int invokeCount = 0;

	@KafkaEntityListener(entity = Product.class, batchSize=10)
	public void readedProducts(Stream<Product> products) {
		
		System.out.println("Received message: " + products);
		
		sum+=products.count();
		invokeCount++;
	}
}
