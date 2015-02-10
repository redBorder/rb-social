package net.redborder.social.twitter;

import net.redborder.social.util.Sensor;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Created by andresgomez on 23/12/14.
 */
public class TwitterSensor extends Sensor {

    public TwitterSensor(String name) {
        super(name);
    }
    public TwitterSensor(Map<? extends String, ? extends Object> m) {
        super(m);
    }

    public void setLocation(List<List<String>> loc){
        setData("location_filters", loc);
    }

    public void setTextFilter(List<String> text_filter){
        setData("text_filters", text_filter);
    }

    public List<List<String>> getLocationFilters(){
        return getData("location_filters");
    }

    public List<String> getTextFilters(){
        return getData("text_filters");
    }

    public String getConsumerKey() {
        return getData("consumer_key");
    }

    public void setConsumerKey(String consumerKey) {
        setData("consumer_key", consumerKey);
    }

    public String getConsumerSecret() {
        return getData("consumer_secret");
    }

    public void setConsumerSecret(String consumerSecret) {
        setData("consumer_secret", consumerSecret);
    }

    public String getTokenKey() {
        return getData("token_key");
    }


    public void setTokenKey(String tokenKey) {
        setData("token_key", tokenKey);
    }

    public String getTokenSecret() {
        return getData("token_secret");
    }

    public void setTokenSecret(String tokenSecret) {
        setData("token_secret", tokenSecret);
    }

    public String getUniqueId(){
        String sensorName = getSensorName() == null ? "sensor_" + UUID.randomUUID().toString() : getSensorName();
        String locationFilters = getLocationFilters() == null ? "locationNone" :  getLocationFilters().toString();
        String textFilters = getTextFilters() == null ? "textFiltersNone" : getTextFilters().toString();

        return sensorName + getConsumerKey() + locationFilters + textFilters;
    }
}
