//Receives a video as a ByteArray through a producer of the given topic and compiles it into a video on the machine in which the code is run
import java.util.Properties;
import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class consumerToVid {
    
    public static void main(String[] args) throws Exception {
    
        //Kafka consumer configuration settings
        String topicName = "interns_test";
        Properties props = new Properties();
        //Assign the IP of the master node to the metadata.broker.list property
	//(6667)
        props.put("metadata.broker.list", "<hostname:portnumber>");
        //Assign the IP's of all the nodes in the cluster along with the port on which zookeeper is running to the zookepper.connect property
	//(2181)
        props.put("zookeeper.connect", "<hostname1:portnumber>,<hostname2:portnumber>,<hostname3:portnumber>");
        //Assign the IP of the master node to the bootstrap.server property
	//(6667)
        props.put("bootstrap.servers", "<hostname:portnumber>");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        //Using a ByteArrayDeserializer as the producer used a ByteArraySerializer to produce the video 
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        //Create an instance of KafkaConsumer
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer <String, byte[]>(props);
        //Kafka Consumer subscribes list of topics here.
        consumer.subscribe(Arrays.asList(topicName));
        //print the topic name
        System.out.println("Subscribed to topic " + topicName);      
        int counter=1;
        //Infinite loop to keep the consuming ongoing
        while (true) {
    	    byte[] b= new byte[100000];
            ConsumerRecords<String, byte[]> records = consumer.poll(100);
            //Loop through all the records in the given poll
            for (ConsumerRecord<String,byte[]> record : records) {
                b=record.value();
                //Print the ByteArray in the terminal
                for(int i=0;i<b.length;i++) {
        	        System.out.println(b[i]);
                }
                //StringBuilder instance renames the consumed files to FinalVideo1, FinalVideo2, etc. with a counter
                StringBuilder sb = new StringBuilder();
                sb.append("<path to output>");
                //Append the counter to the path of the file 
                sb.append(counter);
                sb.append(".mp4");
                String output = sb.toString();
                //Print the output path
                System.out.println(output);
                File someFile = new File(output);
                //Create a ouput stream to the output file
                FileOutputStream out = new FileOutputStream(someFile);
                //Write the ByteArray recieved from the producer into the file
      	        out.write(b);
      	        out.close();
      	        counter++;
            } 
        }
    }
}
