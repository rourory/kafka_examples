package kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class ProducerDemoWithCallback {


    public static void main(String[] args) {
        log.warn("from debug");

        //https://stackoverflow.com/questions/64068185/docker-image-taking-up-space-after-deletion
        //create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //create a producer record and send the data - async operation
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>("demo_java", "another hello from java code" + i);
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e == null) {
                        //success
                        log.info("Receive new metadata/ \n" +
                                "Topic: " + metadata.topic() + "/ \n" +
                                "Partition: " + metadata.partition() + "/ \n" +
                                "Offset: " + metadata.offset() + "/ \n" +
                                "Timestamp: " + metadata.timestamp());
                    } else {
                        log.error("Error while producing", e);
                    }
                }
            });
        }

        //flush data - sync operation (not necessary)
        producer.flush();

        //flush and close the Producer
        producer.close();


    }
}
