//Connects the local machine to a server with given IP and opens the terminal in the console
import com.jcraft.jsch.*;

public class ConnectToCluster {
    
    public static void main(String[] arg) {
        try {
            JSch jsch=new JSch();
            //User to log into 
            String user = "ubuntu";
            //Host IP
            String host = "<IP of Host>";
            //Port on which the connection should be established
            int port = 22;
            //Path to private key if necessary 
            String privateKey = "<Location of PPk file>";
            jsch.addIdentity(privateKey);
            System.out.println("Identity added ");
            Session session = jsch.getSession(user, host, port);
            System.out.println("session created.");
            // disabling StrictHostKeyChecking may help to make connection but makes it insecure
            // see http://stackoverflow.com/questions/30178936/jsch-sftp-security-with-session-setconfigstricthostkeychecking-no
            java.util.Properties config = new java.util.Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.connect();
            Channel channel=session.openChannel("shell");
            // Enable agent-forwarding.
            //((ChannelShell)channel).setAgentForwarding(true);
            channel.setInputStream(System.in);
            /*
            // a hack for MS-DOS prompt on Windows.
            channel.setInputStream(new FilterInputStream(System.in){
                public int read(byte[] b, int off, int len)throws IOException{
                    return in.read(b, off, (len>1024?1024:len));
                }
            });
            */
            channel.setOutputStream(System.out);
            /*
            // Choose the pty-type "vt102".
            ((ChannelShell)channel).setPtyType("vt102");
            */
            /*
            // Set environment variable "LANG" as "ja_JP.eucJP".
            ((ChannelShell)channel).setEnv("LANG", "ja_JP.eucJP");
            */
            //channel.connect();
            channel.connect(3*1000);
        }
        catch(Exception e) {
            System.out.println(e);
        }
    }
}
