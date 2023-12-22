package cn.supcon.mqtt;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class MqttReceive {
    private MqttReceiveCallback mqttReceiveCallback = new MqttReceiveCallback();

    private static int QoS = 1;//通讯的质量，最高是2
    private static String Host = "tcp://127.0.0.1:1883";
    private static MemoryPersistence memoryPersistence = null;
    private static MqttConnectOptions mqttConnectOptions = null;
    private static MqttClient mqttClient = null;

    public void init(String clientId) {
        mqttConnectOptions = new MqttConnectOptions();
        memoryPersistence = new MemoryPersistence();

        if (null != memoryPersistence && null != clientId && null != Host) {
            try {
                mqttClient = new MqttClient(Host, clientId, memoryPersistence);
            } catch (MqttException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        if (null != mqttConnectOptions) {
            mqttConnectOptions.setCleanSession(true);
            mqttConnectOptions.setConnectionTimeout(30);
            mqttConnectOptions.setKeepAliveInterval(45);
            if (null != mqttClient && !mqttClient.isConnected()) {
                //这里可以自己new一个回调函数，比如new MqttReceiveCallback()。我这里使用自动装配，让Spring容器来管理bean与bean的依赖
                mqttClient.setCallback(mqttReceiveCallback);
                try {
                    System.out.println(mqttReceiveCallback);
                    System.out.println("尝试连接");
                    mqttClient.connect();
                } catch (MqttException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }

    public void receive(String topic) {
        int[] Qos = {QoS};
        String[] topics = {topic};

        if (null != mqttClient && mqttClient.isConnected()) {
            if (null != topics && null != Qos && topics.length > 0 && Qos.length > 0) {
                try {
                    System.out.println("订阅主题");

                    mqttClient.subscribe(topics, Qos);
                } catch (MqttException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        } else {
            System.out.println("初始化");
            init("supcon");
            receive(topic);
        }
    }

    public static void main(String[] args) {
        MqttReceive mqttReceive = new MqttReceive();
        mqttReceive.init("supcon");
        mqttReceive.receive("test_topic_20231222");
    }
}
