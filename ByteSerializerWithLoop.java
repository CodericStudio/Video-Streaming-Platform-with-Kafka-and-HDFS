//Passes all the videos of a folder given as a command line argument into a topic using a ByteArraySerializer
//import util.properties packages
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

//import simple producer packages
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;

//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

//Create java class named ByteSerializerWithLoop
public class  ByteSerializerWithLoop {
   
    public static void main(String[] args) throws IOException {
      
        //Assign topicName to string variable
        String topicName = "interns_test";
        // create instance for properties to access producer configs   
        Properties props = new Properties();
        //Assign bootstrap.servers and metadata.broker.list to the primary node in the cluster 
		//(6667)
        props.put("bootstrap.servers", "<hostname:portname>");
		//(6667)
        props.put("metadata.broker.list", "<hostname:portname>");
        //Assign zookeeper.connect to all the nodes in the cluster
		//(2181)
        props.put("zookeeper.connect", "<hostname1:portname>,<hostname2:portname>,<hostname3:portname>");
        //Set acknowledgements for producer requests.      
        props.put("acks", "all");
        //If the request fails, the producer can automatically retry,
        props.put("retries", 0);
        //Specify buffer size in config
        props.put("batch.size", 16384);
        //Reduce the no of requests less than 0   
        props.put("linger.ms", 1);
        //The buffer.memory controls the total amount of memory available to the producer for buffering.   
        props.put("buffer.memory", 33554432);
        //Using a ByteArraySerializer for the value as the video is converted to a ByteArray
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,ByteArraySerializer.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //Initialize a Kafka Producer
        Producer<String, byte[]> producer = new KafkaProducer<String,byte[]>(props);
        //Intializing dir of type File whose value is inputted as a command line argument
        //Command line argument should be the full path of the folder and contain only videos 
        File dir = new File(args[0]);
		File[] directoryListing = dir.listFiles();
		if (directoryListing != null) {
		    //Looping through the videos in the directory 
		    for (File child : directoryListing) {
		        try {	 
                    //Created a ByteArray of the length of size of the Video File
		            byte[] b = new byte[(int) child.length()];
		            //Created a FileinputStream pointing to the video file on the local machine
		            FileInputStream fileInputStream = new FileInputStream(child);
		            //the fileInputStream read the video file as bytes into ByteArray b
		            fileInputStream.read(b);
		            for(int i = 0; i < b.length; i++) {
		         	    System.out.println(b[i]);
		      	    }
		      	    //Sending the ByteArray to Kafka along to the topic held in the varibale name topicName
		        	producer.send(new ProducerRecord<String,byte[]>(topicName, b));
		            fileInputStream.close();
		        }
		        catch (FileNotFoundException e) {
		           	  System.out.println("File Not Found.");
		        	  e.printStackTrace();
		        }
		    	System.out.println(child);
		    }
	    } 
	    else {
		    // Handle the case where dir is not really a directory.
		    // Checking dir.isDirectory() above would not be sufficient
		    // to avoid race conditions with another process that deletes
		    // directories.
		    System.out.println("Directory given doesn't exist");
		}
        System.out.println("Message sent successfully");
        producer.close();
    }
}