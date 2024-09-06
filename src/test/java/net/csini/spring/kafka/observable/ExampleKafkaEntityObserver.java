package net.csini.spring.kafka.observable;

import org.springframework.stereotype.Service;

import io.reactivex.rxjava3.core.Observable;
import lombok.Getter;
import net.csini.spring.kafka.KafkaEntityObservable;
import net.csini.spring.kafka.entity.Place;

@Service
@Getter
public class ExampleKafkaEntityObserver {

	@KafkaEntityObservable(entity = Place.class)
	private Observable<Place> placeObservable;
	
	@KafkaEntityObservable(entity = Place.class, autostart = false)
	private Observable<Place> placeObservableOther;
	
	@KafkaEntityObservable(entity = Place.class)
	private Observable<Place> placeObservableThird;
	
	@KafkaEntityObservable(entity = Place.class, autostart = false)
	private Observable<Place> placeObservableBefore;

}
	