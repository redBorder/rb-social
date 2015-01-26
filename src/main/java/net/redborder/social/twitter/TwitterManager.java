package net.redborder.social.twitter;

import net.redborder.social.util.ConfigFile;
import net.redborder.social.util.Sensor;
import net.redborder.social.util.SensorType;
import net.redborder.taskassigner.MappedTask;
import net.redborder.taskassigner.Task;
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

    private Map<String, LinkedBlockingQueue<String>> msgQueue;

    public TwitterManager() {
        msgQueue = new HashMap<>();
        twitterConsumer = new TwitterConsumer(msgQueue);
        producers = new HashMap<>();
    }

    @Override
    public void updateTasks(List<Task> list) {

        List<Sensor> tasks = ConfigFile.getInstance().getSensors(SensorType.TWITTER);
        List<Sensor> reallyTask = new ArrayList<>();

        for(Task sensorName : list){
            for(Sensor task : tasks){
                MappedTask sensorTask = (MappedTask) sensorName;
                TwitterSensor twitterSensor = new TwitterSensor(sensorTask.asMap());
                if(twitterSensor.getSensorName().equals(task.getSensorName() + "_twitter")){
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


        for (Sensor sensor : reallyTask) {
            TwitterSensor twitterSensor = (TwitterSensor) sensor;
            newTask.add(twitterSensor.asMap().toString());
            twitterSensors.add(twitterSensor);
        }

        taskToRemove.removeAll(newTask);
        newTask.removeAll(runningTask);

        for (TwitterSensor twitterSensor : twitterSensors) {

            if (newTask.contains(twitterSensor.asMap().toString())) {
                TwitterProducer producer = new TwitterProducer(msgQueue.get(twitterSensor.asMap().toString()),
                        twitterSensor.getSensorName(), twitterSensor.getLocationFilters());
                producers.put(twitterSensor.getSensorName(), producer);
                producer.start();
            }

            if (taskToRemove.contains(twitterSensor.asMap().toString())) {
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
