package net.csini.spring.kafka.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import net.csini.spring.kafka.KafkaEntity;

/**
 * @author csini
 *
 */
@Data
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@KafkaEntity(customTopicName = "PRODUCT")
@EqualsAndHashCode(of = "id")
public class Product {

	@net.csini.spring.kafka.KafkaEntityKey
	private String id;

	private String title;

	private String description;
	
}