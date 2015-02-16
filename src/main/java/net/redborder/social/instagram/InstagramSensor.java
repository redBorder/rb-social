package net.redborder.social.instagram;

import net.redborder.social.util.Sensor;

import java.util.List;
import java.util.Map;

/**
 * Created by andresgomez on 29/1/15.
 */
public class InstagramSensor extends Sensor{
    public InstagramSensor(String name) {
        super(name);
    }

    public InstagramSensor(Map<? extends String, ? extends Object> m) {
        super(m);
    }

    public String getClientId(){
        return getData("client_id");
    }

    public String getClientSecret(){
        return getData("client_secret");
    }

    public List<List<String>> getLocationFilter(){
        return getData("location_filter");
    }

    public void setLocationFilter(List<List<String>> locationFilter){
        setData("location_filter", locationFilter);
    }

    public void setClientSecret(String clientSecret){
        setData("client_secret", clientSecret);
    }

    public void setClientId(String clientId){
        setData("client_id", clientId);
    }

    public String getUniqueId(){
        return getSensorName() + getClientId() + getClientSecret() + getLocationFilter().toString();
    }


}
