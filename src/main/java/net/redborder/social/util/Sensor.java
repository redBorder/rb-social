package net.redborder.social.util;

import net.redborder.taskassigner.MappedTask;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by andresgomez on 23/12/14.
 */
public abstract class Sensor extends MappedTask{

    public Sensor(String name) {
        setData("name", name);
    }

    public Sensor(Map<? extends String, ? extends Object> m){
        initializeData(m);
    }

    public String getSensorName() {
        return (String) getData("name");
    }

    public void setSensorName(String name) {
        setData("name", name);
    }

    public void initializeData(Map<? extends String, ? extends Object> m){
        initialize(m);
    }
}
