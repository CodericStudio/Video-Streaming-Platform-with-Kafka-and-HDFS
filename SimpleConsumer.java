//Sets up a Simple Consumer which Consumes messages from a given topic and displays them in the console
import java.util.Properties;
import java.util.Arrays;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class SimpleConsumer {
    
    public static void main(String[] args) throws Exception {
        //Kafka consumer configuration settings
        String topicName = "interns_test";
        Properties props = new Properties();
        //Add the IP of the main node to the bootstrap.server property
	//(6667)
        props.put("bootstrap.servers", "<hostname:portnumber>");
        //Group ID of the comsumer of list of consumers
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        //Add the IP of the main node to the metadata.broker.list property
	//(6667)
        props.put("metadata.broker.list", "<hostname:portnumber>");
        //Add the IP's of all the available nodes in the zookeeper.connect property 
	//(2181)
        props.put("zookeeper.connect", "<hostname1:portnumber>,<hostname2:portnumber>,<hostname3:portnumber>");
        //Using a String deserializer
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        //Create a KafkaConsumer Object
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        //Kafka Consumer subscribes list of topics here.
        consumer.subscribe(Arrays.asList(topicName));
        //print the topic name
        System.out.println("Subscribed to topic " + topicName);
        //Infinite loop to keep the consumer ready to consume messages
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
            // print the offset,key and value for the consumer records.
            System.out.printf("offset = %d, key = %s, value = %s\n",record.offset(), record.key(), record.value());
        }
    }
}
