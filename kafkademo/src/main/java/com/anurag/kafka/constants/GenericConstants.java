package com.anurag.kafka.constants;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class GenericConstants {
	
	public static final String BOOTSTRAP_SERVERS ="bootstrap.servers";
	public static final String BOOTSTRAP_SERVERS_VALUE ="127.0.0.1:9092";
	public static final String KEY_SERIALIZER ="key.serializer";
	public static final String KEY_SERIALIZER_VALUE =StringSerializer.class.getName();
	public static final String VALUE_SERIALIZER ="value.serializer";
	public static final String VALUE_SERIALIZER_VALUE =StringSerializer.class.getName();
	public static final String GROUP_ID ="my-consumer-groupid";
	public static final String EARLIEST ="earliest";
	public static final String KEY_DESERIALIZER_VALUE =StringDeserializer.class.getName();
	public static final String VALUE_DESERIALIZER_VALUE =StringDeserializer.class.getName();

}
