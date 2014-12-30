package net.redborder.social.util.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;

/**
 * Created by crodriguez on 12/4/14.
 */
public class KafkaProducer implements Consumer {

    private ProducerConfig config;
    private Producer<String, String> producer;
    private KafkaBrokers brokers;

    public KafkaProducer(KafkaBrokers brokers) {
        Logger.getRootLogger().setLevel(Level.OFF);
        this.brokers = brokers;
    }

    @Override
    public void prepare() {
        Properties props = new Properties();
        props.put("metadata.broker.list", brokers.get());
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
        props.put("message.send.max.retries", "60");
        props.put("retry.backoff.ms", "1000");
        props.put("producer.type", "async");
        props.put("queue.buffering.max.messages", "10000");
        props.put("queue.buffering.max.ms", "500");

        config = new ProducerConfig(props);
        producer = new Producer<>(config);
    }

    @Override
    public void send(String topic, Object obj) {
        KeyedMessage<String, String> data = new KeyedMessage<>(topic, (String) obj);
        producer.send(data);
    }

    @Override
    public void end() {
        producer.close();
    }

    @Override
    public void reload() {
        this.brokers.reload();
        prepare();
    }
}
