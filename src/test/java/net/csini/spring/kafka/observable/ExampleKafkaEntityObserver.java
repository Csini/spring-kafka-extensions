package net.csini.spring.kafka.observable;

import org.springframework.stereotype.Service;

import io.reactivex.rxjava3.observables.ConnectableObservable;
import lombok.Getter;
import net.csini.spring.kafka.KafkaEntityObservable;
import net.csini.spring.kafka.entity.Place;

@Service
@Getter
public class ExampleKafkaEntityObserver {

	@KafkaEntityObservable(entity = Place.class, groupid = "testgroupid")
	private ConnectableObservable<Place> placeObservable;

}
	