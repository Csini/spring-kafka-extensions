# spring-kafka-extensions library
spring-kafka-extensions is a library that makes an entity based reactive approach (rxjava) to apache kafka possible, if you are using spring

Imagine that you have per Topic one KafkaEntity and you can read (consume) and write (produce) them easyly reactive. 

This library can create automatic an Observer, an Observable and a Subject to every KafkaEntity! All you need to do is to use the custom `@KafkaEntity`, `@KafkaEntityKey`, `@KafkaEntityObserver`, `@KafkaEntityObservable` and `@KafkaEntitySubject` annotations!
# how to install
## add the library as dependency

```html
<dependency>
	<groupId>net.csini.spring.kafka</groupId>
	<artifactId>spring-kafka-extensions</artifactId>
	<version>1.0.0</version>
</dependency>
```
## create a `@Bean` from `KafkaEntityConfig` e.g

```java
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import net.csini.spring.kafka.config.KafkaEntityConfig;
import net.csini.spring.kafka.exception.KafkaEntityException;

@Configuration
public class KafkaEntityEnvironment {

	@Autowired
	ApplicationContext applicationContext;

	@Value(value = "${spring.kafka.bootstrap-servers:localhost:9092}")
	private List<String> bootstrapServers = new ArrayList<>(Collections.singletonList("localhost:9092"));

	@Bean
	public KafkaEntityConfig kafkaEntityConfig() throws KafkaEntityException {

		KafkaEntityConfig kafkaEntityConfig = new KafkaEntityConfig(applicationContext, bootstrapServers);
		kafkaEntityConfig.afterPropertiesSet();
		// here is a good timing to subscribe to KafkaEntityObservers (optional)
		
		List<KafkaEntityException> errors = kafkaEntityConfig.getErrors();
		// handle errors (optional)
		
		return kafkaEntityConfig;
	}
}
```
# how to use
## define per topic one class or record and mark with `@KafkaEntity` (mark a field with `@KafkaEntityKey`) 
Topic name will be the name of the entity class with included packagename
but you can use custom Topic name like this `@KafkaEntity(customTopicName = "PRODUCT")`

```java
package net.csini.spring.kafka.entity;

import net.csini.spring.kafka.KafkaEntity;
import net.csini.spring.kafka.KafkaEntityKey;

@KafkaEntity
public record Student(@KafkaEntityKey String studentid, int age) {

}
```
Topic name will be `net.csini.spring.kafka.entity.Student`
```java
package net.csini.spring.topic;

import net.csini.spring.kafka.KafkaEntity;

@KafkaEntity
public class Product {

	@net.csini.spring.kafka.KafkaEntityKey
	private String id;

	private String title;

	private String description;
	
}
```
Topic name will be `net.csini.spring.topic.Product`

## write data to a topic (produce)
in a Spring Bean just inject a **KafkaEntityObserver** (City as KafkaEntity is defined [here](src/test/java/net/csini/spring/kafka/entity/City.java))

default is `transactional=true`

```java
import java.util.List;

import org.springframework.stereotype.Service;

import io.reactivex.rxjava3.core.Observer;
import net.csini.spring.kafka.KafkaEntityObserver;
import net.csini.spring.kafka.entity.City;

@Service
public class ExampleKafkaEntityObserverService {
	
	@KafkaEntityObserver(entity = City.class)
	private Observer<City> cityObserver;
	
	private List<City> input = List.of(new City("Budapest"), new City("Wien"));

	public void sendCitiesToKafkaTopic(){
		Observable.fromIterable(input).subscribe(cityObserver);
	}
}
```

if you need the **RecordMetadata** from the Kafka Message than you can use KafkaEntitySubject

in a Spring Bean just inject a **KafkaEntitySubject** (User as KafkaEntity is defined [here](src/test/java/net/csini/spring/kafka/entity/User.java))

default is `transactional=true`

```java
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.stereotype.Service;

import io.reactivex.rxjava3.core.Single;
import net.csini.spring.kafka.KafkaEntitySubject;
import net.csini.spring.kafka.entity.User;

import net.csini.spring.kafka.subject.KafkaSubject;

@Service
public class OtherKafkaEntitySubjectService {

	@KafkaEntitySubject(entity = User.class, transactional = false)
	private KafkaSubject<User> userSubject;

	public RecordMetadata sendUserToKafkaTopic(User u) {
		List<RecordMetadata> ret = new ArrayList<>();
		userSubject.subscribe(r -> {
			ret.add(r);
		});
		Single.just(u).toObservable().subscribe(userSubject);
		return ret.get(0);
	}

}
```

## read data from a topic (consume)
in a Spring Bean just inject a **KafkaEntityObservable** (Place as KafkaEntity is defined [here](src/test/java/net/csini/spring/kafka/entity/Place.java))

```java
import org.springframework.stereotype.Service;

import io.reactivex.rxjava3.core.Observable;
import net.csini.spring.kafka.KafkaEntityObservable;
import net.csini.spring.kafka.entity.Place;

@Service
public class ExampleKafkaEntityObservableService {

	@KafkaEntityObservable(entity = Place.class)
	private Observable<Place> placeObservable;
	
	public void readPlacesFromKafkaTopic() throws Exception {

		List<Place> eventList = new ArrayList<>();
		
		@NonNull
		Disposable connect = placeObservable.subscribe(r -> {
			eventList.add(r);
		});
		
		// later if you want to stop consuming, you can unsubscribe
		connect.dispose();
	}
}
```

# example project which is using this library

[https://github.com/Csini/catalog-synchronizer](https://github.com/Csini/catalog-synchronizer)

- version 1.0.0 uses the "traditional" `@KafkaListener` and `KafkaTemplate` from spring-kafka

- version 2.0.0 (ISSUE-35) uses **spring-kafka-extensions** `@KafkaEntity`, `@KafkaEntityKey`, `@KafkaEntityObserver`, `@KafkaEntityObservable` and `@KafkaEntitySubject` annotations!
