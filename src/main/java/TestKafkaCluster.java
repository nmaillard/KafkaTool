import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.apache.curator.test.TestingServer;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.server.KafkaServerStartable;
import kafka.utils.*;


public class TestKafkaCluster {
     KafkaServerStartable kafkaServer;
     TestingServer zkServer;
     
     private static final int BROKER_ID = 5;
     private static final int BROKER_PORT = 59002;

     private static Path tempDirPath = null;
     private static Path logsDirPath = null;

     
  
     public TestKafkaCluster() throws Exception {
    	 System.out.println("going to truc :");
         /*zkServer = new TestingServer();
         System.out.println(" New testing server :"+zkServer.getConnectString());
         zkServer.start();
         KafkaConfig config = getKafkaConfig(zkServer);
         System.out.println(" kafka config:"+config.getString("zookeeper.connect"));
         //KafkaServer ks = 
         KafkaServer server = new KafkaServer(config, kafka.utils.SystemTime$.MODULE$, null);
         kafkaServer = new KafkaServerStartable(config);
         System.out.println(" kafka server :");
         //server.startup();
         kafkaServer.startup();
         System.out.println(" finisehd start up :");*/
    	 this.truc();
     }
     
     private  void truc () throws Exception{
    	 System.out.println("truc :");
    	 zkServer = new TestingServer();
    	    Path tempDirPath = Files.createTempDirectory("kafkatest-");
    	    Path logsDirPath = Files.createDirectory(tempDirPath.resolve("kafka-logs-" + BROKER_ID));
    	    Properties props = new Properties();
    	    props.put("broker.id", BROKER_ID);
    	    props.put("port", BROKER_PORT);
    	    props.put("log.dir", logsDirPath.toAbsolutePath().toString());
    	    props.put("zookeeper.connect", getZkConnectString());
    	    props.put("host.name", "127.0.0.1");
    	    props.put("auto.create.topics.enable", "true");
    	    KafkaConfig config = new KafkaConfig(props);
    	    kafkaServer = new KafkaServerStartable(config);
    	    kafkaServer.startup();
    	    System.out.println(" finisehd start up :");
    	    
    	    if (kafkaServer != null) {
    	        kafkaServer.shutdown();
    	        kafkaServer.awaitShutdown();
    	        kafkaServer = null;
    	    }
    	    System.out.println(" tore down :");
    	    
    	    if (tempDirPath != null && Files.exists(tempDirPath)) {
    	    	File f = tempDirPath.toFile();
    	    			if (f!=null){
    	    				f.delete();
    	    			}
    	        tempDirPath = null;
    	        logsDirPath = null;
    	    }
    	    System.out.println(" cleaned files :");
    	    if (zkServer != null) {
    	        zkServer.stop();
    	        zkServer = null;
    	    }
    	    System.out.println(" stopped ZK:");
     }
  
     private static KafkaConfig getKafkaConfig(final TestingServer zkServer) {
    	 System.out.println(" getKafkaConfig :");
    	// setup Broker
         //int port = TestUtils.choosePort();
         //Properties props = TestUtils.createBrokerConfig(0, port,false);
         Properties propk = new Properties();
         System.out.println(" got prop :");
        		    propk.put("brokerid", "1");
        		    System.out.println(" set borkerId :"+propk.getProperty("brokerid"));
        		    propk.put("port", zkServer.getPort());
        		    propk.put("log.dir", new File("/Users/nmaillard/tkafka").getAbsoluteFile());
        		    System.out.println(" logdir :"+propk.getProperty("log.dir"));
        		    propk.put("log.flush.interval", "1");
        		    System.out.println(" zookeeper.connect :"+zkServer.getConnectString());
        		    propk.put("zookeeper.connect", zkServer.getConnectString());
         System.out.println(" zookeeper.connect props :"+propk.getProperty("zookeeper.connect"));
         return new KafkaConfig(propk);
     }
  
     public String getKafkaBrokerString() {
         return String.format("localhost:%d",
                 kafkaServer.serverConfig().port());
     }
  
     public String getZkConnectString() {
    	 return zkServer.getConnectString();
     }
  
     public int getKafkaPort() {
         return kafkaServer.serverConfig().port();
     }
  
     public void stop() throws IOException {
         kafkaServer.shutdown();
         zkServer.stop();
     }
     
     public void setUp() throws Exception {
    	 //EmbeddedZookeeper zookeeper = new EmbeddedZookeeper();
    	 
    	 
     }
     
     public static void main(String [] args){
    	 //System.out.println(" Start test going to truc:");
    	 try {
    		 System.out.println(" Start test going to truc:");
			TestKafkaCluster tkc = new TestKafkaCluster();
			System.out.println(" ZKconecctionStrint:"+tkc.getZkConnectString() );
			System.out.println(" Kakfa broker:"+tkc.getKafkaBrokerString() );
		} catch (Exception e) {
			System.out.println(" Exceptionr:"+e.getMessage() );
			e.printStackTrace();
		}
    	 
     }
 }