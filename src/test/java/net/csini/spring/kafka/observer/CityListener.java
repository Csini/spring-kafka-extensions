package net.csini.spring.kafka.observer;

import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import lombok.Getter;
import net.csini.spring.kafka.entity.City;

@Service
@Getter
public class CityListener {

	private static final Logger LOGGER = LoggerFactory.getLogger(CityListener.class);
	
	private int count;
	
	private CountDownLatch receivedCounter;

	@KafkaListener(topics = KafkaEntityObserverTest.TOPIC, containerFactory = "kafkaListenerContainerFactory")
	public void listenCity(City city) {
		LOGGER.warn("received: " + city);
		receivedCounter.countDown();
		count++;
	}

	public void init(int size) {
		receivedCounter = new CountDownLatch(size);
		count=0;
	}
	
}
