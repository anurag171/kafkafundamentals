package com.anurag.kafka.producers;

import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.extern.slf4j.Slf4j;

import static com.anurag.kafka.constants.GenericConstants.*;;

@Slf4j
public class ProducerDemo {
	
	private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);
	
	public static void main(String[] args) {		
		int count =1;
		while(count<=5) {
			ProducerHelper.sendMessage(ProducerHelper.createProducer(ProducerHelper.init()), "my_first_iot_topic", String.valueOf(ThreadLocalRandom.current().nextInt(1, 100000)));
			count++;
		}
		log.info("Done");

	}
}
