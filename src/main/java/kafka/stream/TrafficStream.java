package kafka.stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.System.*;

public class TrafficStream {
    public static  Collection<String> parseJson(String data){

        ObjectMapper mapper = new ObjectMapper();

        Map<String,String> map = new HashMap<String,String>();
        try {
            map = mapper.readValue(data, Map.class);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        return map.values();
    }
    public static  void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-linesplit");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream("datatraffic");
        final String regex = "longitude\": ?\"([\\-0-9\\.]+)|latitude\": ?\"([0-9\\.\\-]+)";
        final Pattern pattern = Pattern.compile(regex, Pattern.MULTILINE);

        source.map( (key,value) -> {
            String tempData = parseJson(value).toArray()[17].toString();
            final Matcher matcher = pattern.matcher(tempData.replaceAll("'","\""));
            out.println(tempData.replaceAll("'","\""));
            return new KeyValue<>(key, parseJson(value));
        }).filter((key,value) -> {
            int foo;
            try {
                foo = Integer.parseInt(value.toArray()[10].toString());
            }
            catch (NumberFormatException e)
            {
                foo = 0;
            }
            return foo > 50;

        }).map((key,value) -> {
            return new KeyValue<>(key,"");
        });

        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
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

