package com.kafka.message.server.example.test;

/**
 * Created by osboxes on 2/27/16.
 */

import com.kafka.message.server.example.core.KafkaMailConsumer;
import com.kafka.message.server.example.core.KafkaMailProducer;
import kafka.admin.TopicCommand;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.Time;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.security.JaasUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import scala.Option;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

/**
 * For online documentation see
 *
 * https://github.com/apache/kafka/blob/0.8.2/core/src/test/scala/unit/kafka/utils/TestUtils.scala
 * https://github.com/apache/kafka/blob/0.8.2/core/src/main/scala/kafka/admin/TopicCommand.scala
 * https://github.com/apache/kafka/blob/0.8.2/core/src/test/scala/unit/kafka/admin/TopicCommandTest.scala
 */
public class KafkaProducerTest {

    public static final String MESSAGE = "message";
    public static final String HELLO = "hello";
    @Rule
    public TemporaryFolder folder= new TemporaryFolder();

    //private int brokerId = 0;
    private String topic = "test";
    private String zookeeperChroot = "/kafka-chroot-for-unittest";

    private Properties createBrokerConfigWithDefaults(int numConfigs, String zkConnect){
        return TestUtils.createBrokerConfigs(numConfigs, zkConnect, true,
                false, Option.<SecurityProtocol>empty(), Option.<File>empty(), true, false, false, false).last();
    }

    public int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    @Test
    public void producerTest() throws InterruptedException, IOException {

        // setup Zookeeper
        EmbeddedZookeeper zkServer = new EmbeddedZookeeper();
        String serverLoc = "127.0.0.1:" + zkServer.port();
        ZkUtils zkUtils = ZkUtils.apply(serverLoc,  30000, 30000, JaasUtils.isZkSecurityEnabled());

        // setup Broker
        Properties props = createBrokerConfigWithDefaults(1, serverLoc);
        // setup server
        //String zooKeeperConnect = props.getProperty("zookeeper.connect");
        //props.put("zookeeper.connect", zooKeeperConnect + zookeeperChroot);
        int serverPort = findFreePort();

        props.setProperty("listeners", "PLAINTEXT://" + "127.0.0.1:"+serverPort);
        KafkaConfig kafkaConfig = KafkaConfig.fromProps(props);
        KafkaServer kafkaServer = TestUtils.createServer(kafkaConfig, new MockTime());

        System.out.println("Wait for cache to update");
        while(kafkaServer.metadataCache().getAliveBrokers().isEmpty()){
            // wait to finish
            Thread.sleep(1000);
        }
        System.out.println("Kafka broker initialized");

        // create topic
        String [] arguments = new String[]{"--topic", topic, "--partitions", "1","--replication-factor", "1"};
        TopicCommand.createTopic(zkUtils, new TopicCommand.TopicCommandOptions(arguments));

        List<KafkaServer> servers = new ArrayList<KafkaServer>();
        servers.add(kafkaServer);
        TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(servers), topic, 0, 5000);

        // setup producer
        Properties properties = TestUtils.getProducerConfig(serverLoc);
        new KafkaMailProducer(topic, folder.getRoot().getAbsolutePath(), serverPort).start();

        // deleting zookeeper information to make sure the consumer starts from the beginning
        // see https://stackoverflow.com/questions/14935755/how-to-get-data-from-old-offset-point-in-kafka
        zkUtils.deletePath("/consumers/group0");

        // setup consumer
        KafkaMailConsumer consumer = spy(new KafkaMailConsumer(topic, "earliest", "group0", serverPort));
        consumer.start();

        // put a file to send a message
        FileOutputStream out = new FileOutputStream(folder.newFile(MESSAGE));
        out.write(HELLO.getBytes());
        out.flush();
        out.close();

        // Wait for process to finish
        Thread.sleep(5000);

        ArgumentCaptor<Integer> keyCaptor = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);
        verify(consumer, times(1)).notifyMessage(keyCaptor.capture(), valueCaptor.capture());
        assertEquals(valueCaptor.getValue(), HELLO);

        // cleanup
        //consumer.shutdown();
        kafkaServer.shutdown();
        zkUtils.close();
        zkServer.shutdown();
    }
}