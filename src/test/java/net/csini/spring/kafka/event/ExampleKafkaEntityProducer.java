package net.csini.spring.kafka.event;

import java.util.concurrent.ExecutionException;

import org.springframework.stereotype.Service;

import net.csini.spring.kafka.KafkaEntityException;
import net.csini.spring.kafka.KafkaEntityProducer;
import net.csini.spring.kafka.entity.Product;
import net.csini.spring.kafka.producer.SimpleKafkaProducer;

@Service
public class ExampleKafkaEntityProducer {

	@KafkaEntityProducer(entity = Product.class, clientid = "testExampleKafkaEntityProducer")
	private SimpleKafkaProducer<Product> productProducer;
	
	
	public Long sendEvent(Product p) throws KafkaEntityException, InterruptedException, ExecutionException {
		return productProducer.send(p).get();
	}
	
}
