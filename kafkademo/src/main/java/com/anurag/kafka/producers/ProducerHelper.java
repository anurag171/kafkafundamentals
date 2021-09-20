package com.anurag.kafka.producers;

import static com.anurag.kafka.constants.GenericConstants.BOOTSTRAP_SERVERS_VALUE;
import static com.anurag.kafka.constants.GenericConstants.KEY_SERIALIZER_VALUE;
import static com.anurag.kafka.constants.GenericConstants.VALUE_SERIALIZER_VALUE;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class ProducerHelper {
	
	static Properties kafkaProducerProperties = new Properties();

	public static void sendMessage(KafkaProducer<String, String> producer,String topic,String message) {
		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, message);
		producer.send(producerRecord, new Callback() {

			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				if(null != metadata) {
					System.out.println(String.format("Offset %s Partition %s", metadata.partition(),metadata.offset()));
				}else {
					System.out.println("metadata is null");
				}
				if(null != exception) {
					System.err.println(exception.getMessage());
				}else {
					System.out.println("error is null");
				}
			}
		});
		producer.flush();
	}
	
	public static void sendMessageWithKey(KafkaProducer<String, String> producer,String topic,String key,String message) {
		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic,key, message);
		producer.send(producerRecord, new Callback() {

			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				if(null != metadata) {
					System.out.println(String.format("Offset %s \nPartition %s \nTopic %s \nTimeStamp %s", metadata.partition(),metadata.offset(),metadata.topic(),metadata.timestamp()));
				}else {
					System.out.println("metadata is null");
				}
				if(null != exception) {
					System.err.println(exception.getMessage());
				}else {
					System.out.println("error is null");
				}
			}
		});
		producer.flush();
	}

	public static KafkaProducer<String, String> createProducer(Properties properties){
		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
		return kafkaProducer;
	}

	public static Properties init() {
		kafkaProducerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_VALUE);
		kafkaProducerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KEY_SERIALIZER_VALUE);
		kafkaProducerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VALUE_SERIALIZER_VALUE);
		return kafkaProducerProperties;
	}

}
