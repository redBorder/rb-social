package net.redborder.social.twitter;

import net.redborder.social.util.ConfigFile;
import net.redborder.social.util.SensorType;
import net.redborder.taskassigner.TasksChangedListener;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by andresgomez on 30/12/14.
 */
public class TwitterManager implements TasksChangedListener {

    private TwitterConsumer twitterConsumer;
    private Map<String, TwitterProducer> producers;

    private Map<String, LinkedBlockingQueue<String>> msgQueue = new HashMap<>();

    public TwitterManager() {
        twitterConsumer = new TwitterConsumer(msgQueue);
        producers = new HashMap<>();
    }

    @Override
    public void updateTasks(List<Map<String, Object>> list) {

        List<Map<String, Object>> tasks = ConfigFile.getInstance().getSensors(SensorType.TWITTER);
        List<Map<String, Object>> reallyTask = new ArrayList<>();

        for(Map<String, Object> sensorName : list){
            for(Map<String, Object> task : tasks){
                if(sensorName.get("sensor_name").equals(task.get("name")+"_twitter")){
                    reallyTask.add(task);
                }
            }
        }

        twitterConsumer.updateTasks(reallyTask);

        List<String> newTask = new ArrayList<>();
        List<String> taskToRemove = new ArrayList<>();
        List<String> runningTask = new ArrayList<>(msgQueue.keySet());
        List<TwitterSensor> twitterSensors = new ArrayList<>();

        taskToRemove.addAll(runningTask);


        for (Map<String, Object> sensor : reallyTask) {
            TwitterSensor twitterSensor = new TwitterSensor(sensor);
            newTask.add(twitterSensor.getConsumerKey());
            twitterSensors.add(twitterSensor);
        }

        taskToRemove.removeAll(newTask);
        newTask.removeAll(runningTask);

        for (TwitterSensor twitterSensor : twitterSensors) {

            if (newTask.contains(twitterSensor.getConsumerKey())) {
                TwitterProducer producer = new TwitterProducer(msgQueue.get(twitterSensor.getSensorName()),
                        twitterSensor.getSensorName());
                producers.put(twitterSensor.getSensorName(), producer);
                producer.start();
            }

            if (taskToRemove.contains(twitterSensor.getConsumerKey())) {
                TwitterProducer producer = producers.get(twitterSensor.getSensorName());
                producer.end();
                producers.remove(twitterSensor.getSensorName());
            }

        }
    }

    public void end() {
        twitterConsumer.end();
        for (TwitterProducer producer : producers.values()) {
            producer.end();
        }
    }

    public void reload() {
        for (TwitterProducer producer : producers.values()) {
            producer.reload();
        }
    }
}
