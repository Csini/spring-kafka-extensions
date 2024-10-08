package net.csini.spring.kafka.observer;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import io.reactivex.rxjava3.core.Observer;
import lombok.Getter;
import net.csini.spring.kafka.KafkaEntityObserver;
import net.csini.spring.kafka.entity.City;

@Service
@Getter
public class ExampleKafkaEntityObserverService {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ExampleKafkaEntityObserverService.class);

	@KafkaEntityObserver(entity = City.class)
	private Observer<City> cityObserver;
	
	private List<City> input = List.of(new City("Budapest"), new City("Wien"));

}
