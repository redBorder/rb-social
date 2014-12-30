package net.redborder.social.util;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by andresgomez on 23/12/14.
 */
public abstract class Sensor extends HashMap<String, Object>{

    public Sensor(String name) {
        put("name", name);
    }

    public Sensor(Map<? extends String, ? extends Object> m){
        inizializeData(m);
    }

    public String getSensorName() {
        return (String) get("name");
    }

    public void setSensorName(String name) {
        put("name", name);
    }

    public <T> T getData(String dataId) {
        T value = (T) get(dataId);
        return value;
    }

    public void setData(String dataId, Object dataValue){
        put(dataId, dataValue);
    }

    public void inizializeData(Map<? extends String, ? extends Object> m){
        putAll(m);
    }
}
