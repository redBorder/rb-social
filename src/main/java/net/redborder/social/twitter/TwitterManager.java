package net.redborder.social.twitter;

import net.redborder.social.util.ConfigFile;
import net.redborder.social.util.Logging;
import net.redborder.social.util.Sensor;
import net.redborder.social.util.SensorType;
import net.redborder.clusterizer.MappedTask;
import net.redborder.clusterizer.Task;
import net.redborder.clusterizer.TasksChangedListener;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by andresgomez on 30/12/14.
 */
public class TwitterManager implements TasksChangedListener {

    private TwitterConsumer twitterConsumer;
    private Map<String, TwitterProducer> producers;

    private Map<String, LinkedBlockingQueue<String>> msgQueue;
    List<String> runningTask;

    private Logger logger;

    public TwitterManager() {
        msgQueue = new HashMap<>();
        twitterConsumer = new TwitterConsumer(msgQueue);
        producers = new HashMap<>();
        runningTask = new ArrayList<>();
        logger = Logging.initLogging(this.getClass().getName());
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
        List<TwitterSensor> twitterSensors = new ArrayList<>();

        taskToRemove.addAll(runningTask);

        for (Sensor sensor : reallyTask) {
            TwitterSensor twitterSensor = (TwitterSensor) sensor;
            newTask.add(twitterSensor.getUniqueId());
            twitterSensors.add(twitterSensor);
        }

        taskToRemove.removeAll(newTask);
        newTask.removeAll(runningTask);
        logger.fine("TASK TO REMOVE: " + taskToRemove);
        logger.fine("NEW TASKS: " + newTask);

        runningTask.addAll(newTask);
        runningTask.removeAll(taskToRemove);

        for (TwitterSensor twitterSensor : twitterSensors) {

            if (newTask.contains(twitterSensor.getUniqueId())) {
                logger.fine("Spawning a new Twitter Producer for sensor " + twitterSensor.getUniqueId());
                TwitterProducer producer = new TwitterProducer(msgQueue.get(twitterSensor.getUniqueId()),
                        twitterSensor, twitterSensor.getLocationFilters());
                producers.put(twitterSensor.getSensorName(), producer);
                producer.start();
                logger.fine("Successfully spawned Twitter Producer for sensor " + twitterSensor.getUniqueId());
            }

            if (taskToRemove.contains(twitterSensor.getUniqueId())) {
                logger.fine("Stopping the Twitter Producer for sensor " + twitterSensor.getUniqueId());
                TwitterProducer producer = producers.get(twitterSensor.getSensorName());
                producer.end();
                producers.remove(twitterSensor.getSensorName());
                logger.fine("Twitter Producer for sensor " + twitterSensor.getUniqueId() + " stopped and removed");
            }

        }
    }

    public void end() {
        twitterConsumer.end();
        for (TwitterProducer producer : producers.values()) {
            logger.info("Stopping Twitter producer " + producer.getName());
            producer.end();
        }
    }

    public void reload() {
        for (TwitterProducer producer : producers.values()) {
            logger.info("Reloading Twitter producer " + producer.getName());
            producer.reload();
        }
    }
}
