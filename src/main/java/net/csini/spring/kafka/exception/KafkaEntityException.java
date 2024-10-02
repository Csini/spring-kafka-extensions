package net.csini.spring.kafka.exception;

/**
 * Thrown by spring-kafka-extensions library if there is a problem creating a
 * Kafka Entity Bean
 * 
 * @author Csini
 */
public class KafkaEntityException extends Exception {

	/**
	 * name of the Kafka Entity Bean
	 */
	private String beanName;

	/**
	 * Constructs a new exception with the specified message and a name of the Kafka
	 * Entity Bean, which had the problem
	 * 
	 * @param beanName name of the Kafka Entity Bean
	 * @param message  problem bei creating the Kafka Entity Bean
	 */
	public KafkaEntityException(String beanName, String message) {
		super(message);
		this.beanName = beanName;
	}

	/**
	 * Constructs a new exception with the specified cause and a name of the Kafka
	 * Entity Bean, which had the problem
	 * 
	 * @param beanName name of the Kafka Entity Bean
	 * @param e        problem bei creating the Kafka Entity Bean
	 */
	public KafkaEntityException(String beanName, Exception e) {
		super(e);
		this.beanName = beanName;
	}

	/**
	 * Getter to the name of the Kafka Entity Bean, which had the problem
	 * 
	 * @return name of the Kafka Entity Bean
	 */
	public String getBeanName() {
		return beanName;
	}

	@Override
	public String getMessage() {
		return beanName + ": " + super.getMessage();
	}

}
