package net.csini.spring.kafka;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafkaEntities(basePackages = "net.csini.spring.kafka")
@AutoConfiguration
@EnableKafka
public class SpringKafkaEntityTestApplication {

}
