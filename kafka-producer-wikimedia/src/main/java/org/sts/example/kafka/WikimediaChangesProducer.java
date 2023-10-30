package org.sts.example.kafka;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.consumer.StickyAssignor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {
    public static void main(String[] args) throws InterruptedException {

        var bootstrapServers = "localhost:9092";
        var topic = "wikimedia.recentchange";
        var url = "https://stream.wikimedia.org/v2/stream/recentchange";


        // Create producer properties
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

//        properties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, StickyAssignor.class.getName());
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");


        // Create the Producer
        var producer = new KafkaProducer<String, String>(properties);

        // Define event source
        EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);
        EventSource eventSource = new EventSource.Builder(eventHandler, URI.create(url)).build();

        // Start the producer in another thread
        eventSource.start();

        // We produce for 10 minutes and block the program until then
        TimeUnit.MINUTES.sleep(10);
    }
}
