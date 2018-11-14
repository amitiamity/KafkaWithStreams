package kafka.learn.streams;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

public class Pipe {

	public static void main(String...s) {
		Properties ps = new Properties();
		ps.put(StreamsConfig.APPLICATION_ID_CONFIG,"streams-pipe");
		ps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		ps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		ps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
		final StreamsBuilder builder = new StreamsBuilder();
		
		KStream<String,String> source = builder.stream("streams-plaintext-input");
		source.to("streams-pipe-ouutput");
		
		
		final Topology topology =  builder.build();
		
		System.out.println(topology.describe());
		
		final KafkaStreams  streams =  new KafkaStreams(topology, ps);
		final CountDownLatch  latch = new CountDownLatch(1);
		
		Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
			
			@Override
			public void run() {
				streams.close();
				latch.countDown();
			}
		});
	
		try {
			streams.start();
			latch.await();
		}catch(Throwable e) {
			System.exit(1);
		}
	}
	
	
}
