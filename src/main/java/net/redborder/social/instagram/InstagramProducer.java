package net.redborder.social.instagram;

import net.redborder.social.util.SematriaSentiment;
import net.redborder.social.util.kafka.KafkaProducer;
import net.redborder.social.util.kafka.ZkKafkaBrokers;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * Created by andresgomez on 29/1/15.
 */
public class InstagramProducer extends Thread {

    private KafkaProducer producer;
    private BlockingQueue<String> msgQueue;
    private ObjectMapper mapper;
    private String sensorName;
    private SematriaSentiment semantria;
    private List<List<String>> locations;
    private int sleepPeriod = 60000;

    public InstagramProducer(BlockingQueue<String> msgQueue, String sensorName, List<List<String>> locations) {
        producer = new KafkaProducer(new ZkKafkaBrokers());
        producer.prepare();
        this.msgQueue = msgQueue;
        this.sensorName = sensorName;
        mapper = new ObjectMapper();
        SematriaSentiment.init();
        semantria = null;
        this.locations = locations;
    }

    @Override
    public void run() {
        while (true) {
            try {
                if (semantria == null) {
                    System.out.println("Sending " + msgQueue.size() + " instagram msgs.");
                    while (!msgQueue.isEmpty()) {
                        String msg = msgQueue.take();
                        producer.send("rb_social", msg);
                    }
                } else {
                    List<String> events = null;
                    try {
                        events = semantria.getEvents();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    if(events != null) {
                        for (String msg : events) {
                            producer.send("rb_social", msg);
                        }
                    }
                }
                Thread.sleep(sleepPeriod);
            } catch (InterruptedException e) {
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
}
