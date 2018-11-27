package com.hiya.mq.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

public class RunKafkaProduce
{
    private final Producer<String, String> producer;
    public final static String TOPIC = "HiyatTopic1";

    private RunKafkaProduce()
    {
        Properties props = new Properties();
        // �˴����õ���kafka��broker��ַ:�˿��б�
        props.put("metadata.broker.list", "10.10.51.74:9092");
        // ����value�����л���
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        // ����key�����л���
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "-1");
        producer = new Producer<String, String>(new ProducerConfig(props));
    }

    void produce()
    {
        int messageNo = 1;
        final int COUNT = 101;
        int messageCount = 0;
        while (messageNo < COUNT)
        {
            String key = String.valueOf(messageNo);
            String data = "Hello kafka message :" + key;
            producer.send(new KeyedMessage<String, String>(TOPIC, key, data));
            System.out.println(data);
            messageNo++;
            messageCount++;
        }
        System.out.println("Producer��һ��������" + messageCount + "����Ϣ��");
    }

    public static void main(String[] args)
    {
        new RunKafkaProduce().produce();
    }
}