package net.csini.spring.kafka.observable;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

import net.csini.spring.kafka.EnableKafkaEntities;

@SpringBootApplication
@EnableKafkaEntities(basePackages = "net.csini.spring.kafka")
@AutoConfiguration
@EnableKafka
public class SpringKafkaEntityObservableTestApplication {

}
