package org.sts.example.kafka;

import com.google.gson.Gson;
import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class OpenSearchConsumer {

    public static RestHighLevelClient createOpenSearchClient() {
        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;

        URI connUri = URI.create("http://localhost:9200");
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));
        }

        return restHighLevelClient;
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        var bootstrapServers = "localhost:9092";
        var groupId = "consumer-opensearch-demo";
        var offsetResetConfig = "latest";

        // consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetConfig);

        // create Consumer
        return new KafkaConsumer<>(properties);
    }

    private static String extractId(String json) {
        // gson library
        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    ;

    public static void main(String[] args) throws IOException {
        String index = "wikimedia";
        var topic = "wikimedia.recentchange";

        // first create an OpenSearch Client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        // create our Kafka Clients
        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        // we need to create the index on OpenSearch if it doesn't exist already

        try (openSearchClient; consumer) {
            boolean exists = openSearchClient.indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT);
            if (!exists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest(index);
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("The {} Index has been created!", index);
            } else {
                log.info("The {} Index already exists", index);
            }

            //subscribe the consumer
            consumer.subscribe(Collections.singleton(topic));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

                int recordCount = records.count();
                log.info("Received {} record(s)", records.count());

                for (ConsumerRecord<String, String> record : records) {
                    //send the record into OpenSearch

                    // Strategy 1
                    // define an ID using Kafka Record coordinats
//                    String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                    try {
                        // Strategy 2
                        // we extract the ID from the JSON value
                        String id = extractId(record.value());

                        IndexRequest indexRequest = new IndexRequest(index)
                                .source(record.value(), XContentType.JSON)
                                .id(id);
                        IndexResponse indexResponse = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
                        log.info("Inserted 1 document into OpenSearch with id: {}", indexResponse.getId());
                    } catch (Exception e) {
                        log.error(e.getMessage());
                        log.error("Inserting document has failed. The next data was lost: {}", record.value());
                    }
                }
            }
        }
    }

}
