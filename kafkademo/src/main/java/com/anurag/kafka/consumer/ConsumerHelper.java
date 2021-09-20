package com.anurag.kafka.consumer;

import static com.anurag.kafka.constants.GenericConstants.BOOTSTRAP_SERVERS_VALUE;
import static com.anurag.kafka.constants.GenericConstants.EARLIEST;
import static com.anurag.kafka.constants.GenericConstants.GROUP_ID;
import static com.anurag.kafka.constants.GenericConstants.KEY_DESERIALIZER_VALUE;
import static com.anurag.kafka.constants.GenericConstants.VALUE_DESERIALIZER_VALUE;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class ConsumerHelper {


	static Properties kafkaConsumerProperties = new Properties();

	public static void subscribe(KafkaConsumer<String, String> consumer,String... topicArray) {
		consumer.subscribe(Arrays.asList(topicArray));
		while (true) {
			ConsumerRecords<String,String> consumerRecords= consumer.poll(Duration.ofMillis(100));
			for(ConsumerRecord<String, String> record: consumerRecords) {
				System.out.println(String.format("Key %s \nValue %s \nOffset %s \nPartition %s \nHeader %s\n", record.key(),record.value(),record.offset(),record.partition(),record.headers()));
			}
		}
	}	

	public static KafkaConsumer<String, String> createConsumer(Properties properties){
		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
		return kafkaConsumer;
	}

	public static Properties init() {
		kafkaConsumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_VALUE);
		kafkaConsumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KEY_DESERIALIZER_VALUE);
		kafkaConsumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, VALUE_DESERIALIZER_VALUE);
		kafkaConsumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
		kafkaConsumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, EARLIEST);
		return kafkaConsumerProperties;
	}

	public static void seekAndAssign(String[] topics, KafkaConsumer<String, String> consumer, int partition,long offsetToReadFrom,int batch) {
		TopicPartition topicPartition = new TopicPartition(topics[0], partition);
		consumer.assign(Arrays.asList(topicPartition));
		consumer.seek(topicPartition, offsetToReadFrom);
		int count = 0;
		boolean keepOnReading = true;
		while(keepOnReading) {
			ConsumerRecords<String,String> consumerRecords= consumer.poll(Duration.ofMillis(100));
			for(ConsumerRecord<String, String> record: consumerRecords) {
				count++;
				System.out.println(String.format("Key %s \nValue %s \nOffset %s \nPartition %s \nHeader %s\n", record.key(),record.value(),record.offset(),record.partition(),record.headers()));
				if(count<=batch) {
					keepOnReading=false;
					break;
				}
			}			
		}		
		System.out.println("Done Reading");
	}
}
