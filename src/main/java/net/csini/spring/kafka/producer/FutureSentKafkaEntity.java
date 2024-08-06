package net.csini.spring.kafka.producer;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.RecordMetadata;

public class FutureSentKafkaEntity implements Future<LocalDateTime> {

	private final Future<RecordMetadata> rec;

	public FutureSentKafkaEntity(Future<RecordMetadata> rec) {
		this.rec = rec;
	}

	@Override
	public boolean isCancelled() {
		return false;
	}

	@Override
	public boolean isDone() {
		return true;
	}

	@Override
	public LocalDateTime get(long timeout, TimeUnit unit)
			throws InterruptedException, ExecutionException, TimeoutException {
		return convert();
	}

	private LocalDateTime convert() throws InterruptedException, ExecutionException{

		long epochTimeMillis = rec.get().timestamp();
		Instant instant = Instant.ofEpochMilli(epochTimeMillis);

		// Use the system default time zone
		ZoneId zoneId = ZoneId.systemDefault();
		LocalDateTime localDateTime = instant.atZone(zoneId).toLocalDateTime();
		return localDateTime;
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return false;
	}

	@Override
	public LocalDateTime get() throws InterruptedException, ExecutionException {
		return convert();
	}

}