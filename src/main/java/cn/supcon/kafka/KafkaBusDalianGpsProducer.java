package cn.supcon.kafka;

import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * 模拟大连轨道交通公交车gps原始数据
 */
public class KafkaBusDalianGpsProducer {
    private static Logger logger = LoggerFactory.getLogger(KafkaBusDalianGpsProducer.class);

    private static Producer<Integer, String> producer = null;

    /*kafka集群配置，开kerberos环境的，使用局域网地址，使用单独的开kerberos的kafka组件*/
    private static String SERVERS = "172.16.2.62:9092";
    private static String BROKER_LIST = "172.16.2.62:9092";
    private static String TOPIC_NAME = "topic_dalian_bus_gps1";

    private KafkaBusDalianGpsProducer() {
    }

    private static Producer<Integer, String> getInstance() {
        if (producer == null) {
            producer = createProducer();
        }
        return producer;
    }

    private static Producer<Integer, String> createProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", SERVERS);
        props.put("metadata.broker.list", BROKER_LIST);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<Integer, String> producer = new KafkaProducer<>(props);
        return producer;
    }

    public static void sendMsg(String msg) {
        // 发送数据到kafka,topic
        sendMsg(msg, TOPIC_NAME);
    }

    public static void sendMsg(String msg, String topicVal) {
        // 发送数据到kafka
        Producer<Integer, String> producer = getInstance();
        producer.send(new ProducerRecord<Integer, String>(topicVal, msg), new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception e) {
                if (e != null) {
                    e.printStackTrace();
                } else {
                    System.out.println(metadata.offset());//1
                }
            }
        });
        producer.flush();// 所有缓存记录被立刻发送
    }

    public static void main(String[] args) throws Exception {
        FileReader fileReader = null;
        BufferedReader bufferedReader = null;
        try {
            fileReader = new FileReader("input/bus_dalian_gps.txt");
            bufferedReader = new BufferedReader(fileReader);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                sendMsg(line);
                // 控制kafka每1分钟发送一次数据
                Thread.sleep(2000);
            }
        } catch (IOException e) {
            logger.info(e.toString());
        } finally {
            if (bufferedReader != null) {
                try {
                    bufferedReader.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (fileReader != null) {
                try {
                    fileReader.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
