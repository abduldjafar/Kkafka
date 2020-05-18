package kafka.stream;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import kafka.producer.AvroSchema;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.System.exit;
import static java.lang.System.out;

public class TrafficStreamAvro {

    public static void main(String[] args) {
        AvroSchema avroSchema = new AvroSchema();
        Schema.Parser parser = new Schema.Parser();

        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-avro");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
       streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        streamsConfiguration.put("schema.registry.url", "http://localhost:8081");

        Schema schema = parser.parse(avroSchema.schemaStringGeo);

        // When you want to override serdes explicitly/selectively
        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url","http://localhost:8081");
        final Serde<GenericRecord> keyGenericAvroSerde = new GenericAvroSerde();

        keyGenericAvroSerde.configure(serdeConfig, true); // `true` for record keys
        final Serde<GenericRecord> valueGenericAvroSerde = new GenericAvroSerde();
        valueGenericAvroSerde.configure(serdeConfig, false); // `false` for record values

        StreamsBuilder builder = new StreamsBuilder();

        KStream<GenericRecord, GenericRecord> textLines = builder.stream("tetsing");
        textLines.map((key, value) -> {

            Double lontitude = Double.valueOf(value.get("Location").toString().
                    replaceAll("\"", "").split(",")[0].split(":")[1]);
            Double lattitude = Double.valueOf(value.get("Location").toString().
                    replaceAll("\"", "").split(",")[5].split(":")[1].split("}")[0]);

            GenericRecord customer = new GenericData.Record(schema);

            customer.put("longitude", lontitude);
            customer.put("lattitude", lattitude);

            return new KeyValue<>(key, customer);
        });



        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);
        final CountDownLatch latch = new CountDownLatch(1);

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
        } catch (Throwable e) {
            exit(1);
        }
        exit(0);
    }

    }

