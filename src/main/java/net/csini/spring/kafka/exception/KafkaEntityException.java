package net.csini.spring.kafka.exception;

public class KafkaEntityException extends Exception {

	private String beanName;
	
	public KafkaEntityException(String beanName, String message) {
		super(message);
		this.beanName = beanName;
	}

	public KafkaEntityException(String beanName, Exception e) {
		super(e);
		this.beanName = beanName;
	}

	public String getBeanName() {
		return beanName;
	}

	@Override
	public String getMessage() {
		return beanName + ": " + super.getMessage();
	}
	
	

}
