//Program that copies a file from the local system to a path in HDFS
//Both command line arguments should be valid existing 
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

public class CopyfromLocal {
 
    public static void main(String[] args) throws IOException, URISyntaxException {
        //Create new config
    	Configuration configuration = new Configuration();
    	//Add Path of the core-site.xml and hdfs-site.xml on the server
    	configuration.addResource(new Path("/etc/hadoop/2.5.5.0-157/0/core-site.xml"));
    	configuration.addResource(new Path("/etc/hadoop/2.5.5.0-157/0/hdfs-site.xml"));
	//(8020)
    	FileSystem fs = FileSystem.get(new URI("hdfs://<hostname:portnumber>"), configuration);
    	fs.copyFromLocalFile(new Path(args[0]), new Path(args[1]));
    }
}
