package cn.supcon.mqtt;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

/**
 * MQTT客户端（发布者）
 */
public class SendMqtt {
    public static MqttClient mqttClient = null;
    private static MemoryPersistence memoryPersistence = null;
    private static MqttConnectOptions mqttConnectOptions = null;

    static {
        init("supcon");
    }

    /**
     * 初始化和创建连接
     */
    public static void init(String clientId) {
        // 初始化连接设置对象
        mqttConnectOptions = new MqttConnectOptions();
        // 初始化MqttClient
        if (null != mqttConnectOptions) {
            // true可以安全地使用内存持久性作为客户端断开连接时清除的所有状态
            mqttConnectOptions.setCleanSession(true);
            // 设置连接超时
            mqttConnectOptions.setConnectionTimeout(30);
            // 设置持久化方式
            memoryPersistence = new MemoryPersistence();
            if (null != memoryPersistence && null != clientId) {
                try {
                    mqttClient = new MqttClient("tcp://127.0.0.1:1883", clientId, memoryPersistence);
                } catch (MqttException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            } else {

            }
        } else {
            System.out.println("mqttConnectOptions对象为空");
        }
        System.out.println(mqttClient.isConnected());
        if (null != mqttClient) {
            if (!mqttClient.isConnected()) {
                try {
                    System.out.println("创建连接");
                    mqttClient.connect(mqttConnectOptions);
                } catch (MqttException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

            }
        } else {
            System.out.println("mqttClient为空");
        }
        System.out.println(mqttClient.isConnected());
    }

    // 与MQTT服务器断开连接
    public void closeConnect() {
        //关闭存储方式
        if (null != memoryPersistence) {
            try {
                memoryPersistence.close();
            } catch (MqttPersistenceException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } else {
            System.out.println("memoryPersistence is null");
        }

        //关闭连接
        if (null != mqttClient) {
            if (mqttClient.isConnected()) {
                try {
                    mqttClient.disconnect();
                    mqttClient.close();
                } catch (MqttException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            } else {
                System.out.println("mqttClient is not connect");
            }
        } else {
            System.out.println("mqttClient is null");
        }
    }

    // 重新连接MQTT服务器
    public void reConnect() {
        if (null != mqttClient) {
            if (!mqttClient.isConnected()) {
                if (null != mqttConnectOptions) {
                    try {
                        mqttClient.connect(mqttConnectOptions);
                    } catch (MqttException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                } else {
                    System.out.println("mqttConnectOptions is null");
                }
            } else {
                System.out.println("mqttClient is null or connect");
            }
        } else {
            init("supcon");//其实就是重新回到初始化的方法中去尝试连接服务器
        }
    }

    //    发布消息
    public void publishMessage(String pubTopic, String message, int qos) {
        if (null != mqttClient && mqttClient.isConnected()) {
            System.out.println("发布消息   " + mqttClient.isConnected());
            System.out.println("id:" + mqttClient.getClientId());
            MqttMessage mqttMessage = new MqttMessage();
            mqttMessage.setQos(qos);
            mqttMessage.setPayload(message.getBytes());

            MqttTopic topic = mqttClient.getTopic(pubTopic);

            if (null != topic) {
                try {
                    MqttDeliveryToken publish = topic.publish(mqttMessage);
                    if (!publish.isComplete()) {
                        System.out.println("消息发布成功");
                    }
                } catch (MqttException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

        } else {
            reConnect();
        }

    }

    //    订阅主题
    public void subTopic(String topic) {
        if (null != mqttClient && mqttClient.isConnected()) {
            try {
                mqttClient.subscribe(topic, 1);
            } catch (MqttException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } else {
            System.out.println("mqttClient is error");
        }
    }


    //    清空主题
    public void cleanTopic(String topic) {
        if (null != mqttClient && !mqttClient.isConnected()) {
            try {
                mqttClient.unsubscribe(topic);
            } catch (MqttException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } else {
            System.out.println("mqttClient is error");
        }
    }

    public static void main(String[] args) throws Exception {
        SendMqtt send = new SendMqtt();
        for (int i = 10; i < 30; i++) {
            send.publishMessage("test_topic_20231222", "物联网MQTT协议第" + i + "条topic信息,载入历史", 1);
            Thread.sleep(3000L);
        }

    }
}
