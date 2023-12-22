package cn.supcon.mqtt;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class MqttReceiveCallback implements MqttCallback {
    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        String msg = new String(message.getPayload());
        System.out.println("Client 接收消息主题 : " + topic);
        System.out.println("Client 接收消息Qos : " + message.getQos());
        System.out.println("Client 接收消息内容 : " + msg);
    }
    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
    }
    @Override
    public void connectionLost(Throwable cause) {
    }

}
