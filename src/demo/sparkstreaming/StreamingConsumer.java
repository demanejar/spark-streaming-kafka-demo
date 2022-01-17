package demo.sparkstreaming;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import demo.sparkstreaming.properties.SSKafkaProperties;

public class StreamingConsumer {
	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Spark Streaming Consumer");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
		
		// Define a list of Kafka topic to subscribe
		Collection<String> topics = Arrays.asList("topic-1");
		
		// Create an input DStream which consume message from Kafka topics
		JavaInputDStream<ConsumerRecord<String, String>> stream;
		stream = KafkaUtils.createDirectStream(jssc, 
				LocationStrategies.PreferConsistent(), 
				ConsumerStrategies.Subscribe(topics, SSKafkaProperties.getInstance()));
		
		JavaDStream<String> lines = stream.map((Function<ConsumerRecord<String,String>, String>) kafkaRecord -> kafkaRecord.value());
		lines.cache().foreachRDD(line -> {
			List<String> list = line.collect();
			if(line != null) {
				for(String l: list) {
					System.out.println(l);
				}
			}
		});
		
		// Start the computation
        jssc.start();
        jssc.awaitTermination();
	}
}
