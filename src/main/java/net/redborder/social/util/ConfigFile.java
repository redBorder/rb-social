/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.social.util;

import net.redborder.social.twitter.TwitterSensor;
import org.ho.yaml.Yaml;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author andresgomez
 */
public class ConfigFile {

    private static ConfigFile theInstance = null;
    private static final Object initMonitor = new Object();
    private final String CONFIG_FILE_PATH = "/opt/rb/etc/rb-social/config.yml";
    private Map<SensorType, List<Sensor>> _sensors;
    private Map<String, Object> _general;

    public static ConfigFile getInstance() {
        if (theInstance == null) {
            synchronized (initMonitor) {
                try {
                    while (theInstance == null) {
                        initMonitor.wait();
                    }
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        }
        return theInstance;
    }

    public static void init() throws FileNotFoundException {
        synchronized (initMonitor) {
            if (theInstance == null) {
                theInstance = new ConfigFile();
                initMonitor.notifyAll();
            }
        }
    }

    /**
     * Constructor
     */
    public ConfigFile() throws FileNotFoundException {
        reload();
    }

    public void reload() throws FileNotFoundException {
        _sensors = new HashMap<>();

            Map<String, Object> map = (Map<String, Object>) Yaml.load(new File(CONFIG_FILE_PATH));

            /* Production Config */
            List<Map<String, Object>> sensors = (List<Map<String, Object>>) map.get("sensors");

            List<Sensor> twitterList = new ArrayList<>();
            List<Sensor> facebookList = new ArrayList<>();

            for (Map<String, Object> sensor : sensors) {
                String sensorType = (String) sensor.get("type");
                switch (sensorType) {
                    case "twitter":
                        String consumer_key = (String) sensor.get("consumer_key");
                        String consumer_secret = (String) sensor.get("consumer_secret");
                        String token_key = (String) sensor.get("token_key");
                        String token_secret = (String) sensor.get("token_secret");
                        String sensor_name = (String) sensor.get("sensor_name");
                        List<List<String>> locations_filters = (List<List<String>>) sensor.get("location_filter");
                        List<String> texts_filter = (List<String>) sensor.get("text_filter");


                        TwitterSensor conf = new TwitterSensor(sensor_name);
                        conf.setConsumerKey(consumer_key);
                        conf.setConsumerSecret(consumer_secret);
                        conf.setTokenKey(token_key);
                        conf.setTokenSecret(token_secret);

                        if (locations_filters != null) {
                            conf.setLocation(locations_filters);
                        }

                        if (texts_filter != null) {
                            conf.setTextFilter(texts_filter);
                        }

                        twitterList.add(conf);
                        break;
                    case "facebook":
                        break;

                }
            }

            _sensors.put(SensorType.TWITTER, twitterList);
            /* General Config */
            _general = (Map<String, Object>) map.get("general");
    }

    public <T> T getSensors(SensorType sensorType) {
        T sensors = (T) _sensors.get(sensorType);
        return sensors;
    }

    public String getZkConnect() {
        return (String) getFromGeneral("zk_connect");
    }

    /**
     * Getter.
     *
     * @param property Property to read from the general section
     * @return Property read
     */

    public <T> T getFromGeneral(String property) {
        T ret = null;

        if (_general != null) {
            ret = (T) _general.get(property);
        }

        return ret;
    }
}
