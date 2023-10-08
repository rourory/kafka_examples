package kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class ConsumerDemoWithShutdown {


    public static void main(String[] args) {
        log.warn("debug from Kafka Consumer with shutdown");

        var bootstrapServers = "localhost:9092";
        var groupId = "third-group";
        var offsetResetConfig = "earliest";
        var topic = "demo_java";

        // Consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetConfig);


        // Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);


        // Get a ref to the current thread
        final Thread mainThread = Thread.currentThread();

        // Adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup();

                // Join main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            // Subscribe consumer to out topic(s)
            consumer.subscribe(Collections.singletonList(topic));

            // Poll for new data
            while (true) {

                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("\n----------------- NEW MESSAGE -----------------\n" +
                            "Key: " + record.key() + "\n" +
                            "Value: " + record.value() + "\n" +
                            "Topic: " + record.topic() + "\n" +
                            "Partition: " + record.partition() + "\n" +
                            "Offset: " + record.offset() + "\n" +
                            "---------------- END MESSAGE ---------------");
                }

            }

        } catch (WakeupException e) {
            log.info("Wakeup exception");
            //we ignore this as this is an expected exception whe closing a consumer
        } catch (Exception e) {
            log.error("Unexpected exception");
            log.error(e.getMessage());
        } finally {
            consumer.close(); // this will also commit the offsets if need be
            log.info("Consumer is now gracefully closed");
        }


    }
}

























