package net.redborder.social.instagram;

import net.redborder.social.util.SematriaSentiment;
import net.redborder.social.util.kafka.KafkaProducer;
import net.redborder.social.util.kafka.ZkKafkaBrokers;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * Created by fernandodominguez on 29/1/15.
 */
public class InstagramProducer extends Thread {

    private KafkaProducer producer;
    private BlockingQueue<String> msgQueue;
    private ObjectMapper mapper;
    private InstagramSensor sensor;
    private SematriaSentiment semantria;
    private long sleepPeriod = 60000;
    private Logger l;

    public InstagramProducer(BlockingQueue<String> msgQueue, InstagramSensor sensor, List<List<String>> locations) {

        producer = new KafkaProducer(new ZkKafkaBrokers());
        producer.prepare();
        this.msgQueue = msgQueue;
        this.sensor = sensor;
        mapper = new ObjectMapper();
        SematriaSentiment.init();
        semantria = SematriaSentiment.getInstance();
        l = Logger.getLogger(InstagramProducer.class.getName());
    }

    @Override
    public void run() {
        while (true) {
            try {
                if (semantria != null) {
                    List<String> events = null;
                    try {
                        events = semantria.getEvents("instagram");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    if (events != null) {
                        for (String msg : events) {

                            Map<String, Object> hashtagMap = hashtagsFromMsg(msg);
                            Map<String, Object> mentionsMap = mentionsFromMsg(msg);

                            if (hashtagMap != null) {
                                l.info("Sending " + mapper.writeValueAsString(hashtagMap) + " to rb_hashtag");
                                producer.send("rb_hashtag", mapper.writeValueAsString(hashtagMap));
                            }

                            if (mentionsMap != null) {
                                l.info("Sending " + mapper.writeValueAsString(mentionsMap) + " to rb_hashtag");
                                producer.send("rb_hashtag", mapper.writeValueAsString(mentionsMap));
                            }

                            producer.send("rb_social", msg);
                        }
                    }
                }else{

                    l.debug("Sending " + msgQueue.size() + " instagram msgs");
                    while (!msgQueue.isEmpty()) {

                        String msg = msgQueue.take();

                        Map<String, Object> hashtagMap = hashtagsFromMsg(msg);
                        Map<String, Object> mentionsMap = mentionsFromMsg(msg);

                        if (hashtagMap != null) {
                            l.info("Sending " + mapper.writeValueAsString(hashtagMap) + " to rb_hashtag");
                            producer.send("rb_hashtag", mapper.writeValueAsString(hashtagMap));
                        }

                        if (mentionsMap != null) {
                            l.info("Sending " + mapper.writeValueAsString(mentionsMap) + " to rb_hashtag");
                            producer.send("rb_hashtag", mapper.writeValueAsString(mentionsMap));
                        }

                        // rb_social msg send
                        producer.send("rb_social", msg);
                        l.debug("Message sent to Kafka: " + msg);
                    }
                }

                Thread.sleep(sleepPeriod);
            } catch (InterruptedException e) {
                l.warn("InstagramProducer thread " + this.sensor.getUniqueId() + " interrupted");
            } catch (IOException e) {
                l.error("IO Exception on InstagramProducer thread " + this.sensor.getUniqueId());
                e.printStackTrace();
            }
        }

    }

    public void reload() {
        producer.reload();
    }

    public void end() {
        producer.end();
    }

    private Map<String, Object> hashtagsFromMsg(String msg){

        JsonNode rootNode;
        JsonNode hashtagNode;
        String[] hashtags;
        Map<String, Object> hashMap = null;

        try {
            rootNode = new ObjectMapper().readTree(new StringReader(msg));
            hashtagNode = rootNode.get("hashtags");
            if (hashtagNode != null) {
                hashtags = hashtagNode.getTextValue().split(" ");
                for (String hash : hashtags) {
                    hashMap = new HashMap<>();
                    hashMap.put("type", "hashtag");
                    hashMap.put("value", hash);
                    hashMap.put("sensor_name", this.sensor.getSensorName());
                    hashMap.putAll(this.sensor.getEnrichment());
                    hashMap.put("timestamp", System.currentTimeMillis() / 1000);
                }
            }
        }catch (IOException e){
            l.error("IOException while parsing hashtags");
            e.printStackTrace();
        }

        return hashMap;
    }

    private Map<String, Object> mentionsFromMsg(String msg){

        JsonNode rootNode;
        JsonNode mentionNode;
        String[] mentions;
        Map<String, Object> hashMap = null;

        try {
            rootNode = new ObjectMapper().readTree(new StringReader(msg));
            mentionNode = rootNode.get("mentions");
            if (mentionNode != null) {
                mentions = mentionNode.getTextValue().split(" ");
                for (String mention : mentions) {
                    hashMap = new HashMap<>();
                    hashMap.put("type", "user_mention");
                    hashMap.put("value", mention);
                    hashMap.put("sensor_name", this.sensor.getSensorName());
                    hashMap.putAll(this.sensor.getEnrichment());
                    hashMap.put("timestamp", System.currentTimeMillis() / 1000);
                }
            }
        }catch (IOException e){
            l.error("IOException while parsing mentions");
            e.printStackTrace();
        }

        return hashMap;
    }
}
