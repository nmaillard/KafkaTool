import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import kafka.admin.AdminUtils;
import kafka.admin.TopicCommand;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerConnector;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.TestUtils;
import kafka.utils.ZkUtils;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class KafkaTest {
	private static final int BROKER_ID = 5;
    private static final int BROKER_PORT = 59002;

    private static Path tempDirPath = null;
    private static Path logsDirPath = null;

    private static TestingServer zkServer = null;
    private static KafkaServerStartable kafkaServer = null;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	    zkServer = new TestingServer();
	    tempDirPath = Files.createTempDirectory("kafkatest-");
	    logsDirPath = Files.createDirectory(tempDirPath.resolve("kafka-logs-" + BROKER_ID));
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
	}
	private static String getZkConnectString() {
	    return zkServer.getConnectString();
	}
	
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	    if (kafkaServer != null) {
	        kafkaServer.shutdown();
	        kafkaServer.awaitShutdown();
	        kafkaServer = null;
	    }
	    if (tempDirPath != null && Files.exists(tempDirPath)) {
	    	tempDirPath.toFile().delete();
	        //FileUtils.deleteDirectory(tempDirPath.toFile());
	        tempDirPath = null;
	        logsDirPath = null;
	    }
	    if (zkServer != null) {
	        zkServer.stop();
	        zkServer = null;
	    }
	}
	
	@Test
	public void CheckTopic() throws Exception {
		ZkClient zkClient = new ZkClient(zkServer.getConnectString(), 10000, 10000);
		ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zkServer.getConnectString()), false);
		AdminUtils.createTopic(zkUtils, "mytopic", 10, 1, new Properties());
		assert(AdminUtils.topicExists(zkUtils, "mytopic"));
	}
	
	@Test
	public void CheckProducer() throws Exception {
        Properties properties = TestUtils.getProducerConfig(zkServer.getConnectString());
        ProducerConfig producerConfig = new ProducerConfig(properties);
        Producer producer = new Producer(producerConfig);
        
       // KeyedMessage<String, String> message = new KeyedMessage<>("mytopic", event.getCustomerId(), serialisedEvent);
        //producer.send(message);
        
     // setup simple consumer
        Properties consumerProperties = TestUtils.createConsumerProperties(zkServer.getConnectString(), "group0", "consumer0", -1);
        ConsumerConnector consumer = (ConsumerConnector) kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProperties));
	}

}
