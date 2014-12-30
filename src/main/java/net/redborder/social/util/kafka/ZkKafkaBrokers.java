package net.redborder.social.util.kafka;

import net.redborder.social.util.ConfigFile;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by crodriguez on 5/12/14.
 */
public class ZkKafkaBrokers implements KafkaBrokers {

    RetryPolicy retryPolicy;
    CuratorFramework client;
    String brokerList;

    public ZkKafkaBrokers() {
        retryPolicy = new ExponentialBackoffRetry(1000, 3);
        readZK();
    }

    public void readZK() {
        String zkHosts = ConfigFile.getInstance().getZkConnect();
        client = CuratorFrameworkFactory.newClient(zkHosts, retryPolicy);
        client.start();

        List<String> ids = null;
        boolean first = true;
        brokerList = "";

        try {
            ids = client.getChildren().forPath("/brokers/ids");
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        if (ids != null) {
            for (String id : ids) {
                String jsonString = null;

                try {
                    jsonString = new String(client.getData().forPath("/brokers/ids/" + id), "UTF-8");
                } catch (Exception ex) {
                    ex.printStackTrace();
                }

                if (jsonString != null) {
                    ObjectMapper mapper = new ObjectMapper();
                    Map<String, Object> json;

                    try {
                        json = mapper.readValue(jsonString, Map.class);

                        if (first) {
                            brokerList = brokerList.concat(json.get("host") + ":" + json.get("port"));
                            first = false;
                        } else {
                            brokerList = brokerList.concat("," + json.get("host") + ":" + json.get("port"));
                        }
                    } catch (NullPointerException | IOException e) {
                        Logger.getLogger(ZkKafkaBrokers.class.getName()).log(Level.SEVERE, "Failed converting a JSON tuple to a Map class", e);
                    }
                }
            }
        }

        System.out.println("Kafka brokers @ " + brokerList);
        client.close();
    }

    @Override
    public String get() {
        return brokerList;
    }

    @Override
    public void reload() {
        readZK();
    }
}
