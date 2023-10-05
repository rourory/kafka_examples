package kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class ConsumerDemo {


    public static void main(String[] args) {
        log.warn("debug from Kafka Consumer");

        var bootstrapServers = "localhost:9092";
        var groupId = "second-group";
        var offsetResetConfig = "earliest";
        var topic = "demo_java";

        // consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetConfig);


        // create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // subscribe consumer to out topic(s)
        consumer.subscribe(Collections.singletonList(topic));

        // poll for new data
        while (true) {
            log.info("Polling...");
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
    }
}

























