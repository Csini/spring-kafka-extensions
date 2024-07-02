package net.csini.spring.kafka.event;

import org.springframework.stereotype.Service;

import net.csini.spring.kafka.KafkaEntityException;
import net.csini.spring.kafka.KafkaEntityProducer;
import net.csini.spring.kafka.entity.Product;
import net.csini.spring.kafka.producer.SimpleKafkaProducer;

@Service
public class ExampleKafkaEntityProducer {

	@KafkaEntityProducer(entity = Product.class)
	private SimpleKafkaProducer<Product> productProducer;
	
	
	public boolean sendEvent(Product p) throws KafkaEntityException {
		productProducer.send(p);
		
		//TODO
		return true;
	}
	
}
