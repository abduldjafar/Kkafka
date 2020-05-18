package kafka.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;
import preproc.CsvToJson;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class Traffic {
    private static class DemoProducerCallback implements Callback {
        // use this function for multhread produce messages

        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                e.printStackTrace();
            }
        }
    }

    private static Properties props(){
        Properties prop = new Properties();
        prop.put("bootstrap.servers", "localhost:9092");
        prop.put("acks", "all");
        // prop.put("delivery.timeout.ms", 30000);
        //prop.put("linger.ms", 1);
        prop.put("compression.type","snappy");
        prop.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        prop.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        prop.put("schema.registry.url", "http://localhost:8081");
        return prop;
    }


    public static void main(String[] args) throws IOException {
        AvroSchema avroSchema = new AvroSchema();
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(avroSchema.schemaString);
        System.out.println(avroSchema.schemaString);

        Properties prop = props();
        Producer<String, GenericRecord> producer = new KafkaProducer<String, GenericRecord>(prop);
        List<Map<String, ?>> data = CsvToJson.Convert("/home/kotekaman/Documents/alldata/traffic-data.csv");

        for (Map<?,?> element : data) {
            ObjectMapper objectMapper = new ObjectMapper();
            GenericRecord customer = new GenericData.Record(schema);
            customer = avroSchema.SetupTrafficScheme(customer,element);
            ProducerRecord<String, GenericRecord> record = new ProducerRecord<>("tetsing",customer );
            producer.send(record, new DemoProducerCallback());
        }

        producer.close();
    }
}

