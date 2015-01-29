/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.redborder.social.util;

import net.redborder.social.instagram.InstagramSensor;
import net.redborder.social.twitter.TwitterSensor;
import net.redborder.taskassigner.MappedTask;
import net.redborder.taskassigner.Task;
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
    private Map<SensorType, List<Sensor>> _sensorNames;

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
        _sensorNames = new HashMap<>();

        Map<String, Object> map = (Map<String, Object>) Yaml.load(new File(CONFIG_FILE_PATH));

            /* Production Config */
        List<Map<String, Object>> sensors = (List<Map<String, Object>>) map.get("sensors");

        List<Sensor> twitterList = new ArrayList<>();
        List<Sensor> instagramList = new ArrayList<>();

        List<Sensor> twitterSensorNames = new ArrayList<>();
        List<Sensor> instagramSensorNames = new ArrayList<>();

        if (sensors != null) {
            for (Map<String, Object> sensor : sensors) {
                String sensorType = (String) sensor.get("type");
                switch (sensorType) {
                    case "twitter":

                        String consumer_key = (String) sensor.get("consumer_key");
                        String consumer_secret = (String) sensor.get("consumer_secret");
                        String token_key = (String) sensor.get("token_key");
                        String token_secret = (String) sensor.get("token_secret");
                        String sensor_name_twitter = (String) sensor.get("sensor_name");
                        List<List<String>> locations_filters_twitter = (List<List<String>>) sensor.get("location_filter");
                        List<String> texts_filter = (List<String>) sensor.get("text_filter");

                        Sensor sensorNameTwitter = new TwitterSensor(sensor_name_twitter + "_twitter");

                        TwitterSensor conf_twitter = new TwitterSensor(sensor_name_twitter);
                        conf_twitter.setConsumerKey(consumer_key);
                        conf_twitter.setConsumerSecret(consumer_secret);
                        conf_twitter.setTokenKey(token_key);
                        conf_twitter.setTokenSecret(token_secret);

                        if (locations_filters_twitter != null) {
                            conf_twitter.setLocation(locations_filters_twitter);
                        }

                        if (texts_filter != null) {
                            conf_twitter.setTextFilter(texts_filter);
                        }

                        twitterSensorNames.add(sensorNameTwitter);
                        twitterList.add(conf_twitter);

                        break;

                    case "instagram":

                        String client_id = (String) sensor.get("client_id");
                        String client_secret = (String) sensor.get("client_secret");
                        String callback_url = (String) sensor.get("callback_url");
                        String sensor_name_instagram = (String) sensor.get("sensor_name");
                        List<List<String>> locations_filters_instagram = (List<List<String>>) sensor.get("location_filter");

                        Sensor sensorNameInstagram = new InstagramSensor(sensor_name_instagram + "_instagram");

                        InstagramSensor conf_instagram = new InstagramSensor(sensor_name_instagram);

                        instagramSensorNames.add(sensorNameInstagram);

                        conf_instagram.setClientId(client_id);
                        conf_instagram.setClientSecret(client_secret);
                        conf_instagram.setCallbackUrl(callback_url);
                        conf_instagram.setLocationFilter(locations_filters_instagram);

                        instagramList.add(conf_instagram);

                        break;
                }
            }
        }

        _sensorNames.put(SensorType.TWITTER, twitterSensorNames);
        _sensors.put(SensorType.TWITTER, twitterList);


        _sensorNames.put(SensorType.INSTAGRAM, instagramSensorNames);
        _sensors.put(SensorType.INSTAGRAM, instagramList);
            /* General Config */
        _general = (Map<String, Object>) map.get("general");
    }

    public <T> T getSensors(SensorType sensorType) {
        T sensors = (T) _sensors.get(sensorType);
        return sensors;
    }

    public List<Sensor> getSensorNames(SensorType type) {
        return _sensorNames.get(type);
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
