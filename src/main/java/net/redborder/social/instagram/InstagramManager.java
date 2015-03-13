package net.redborder.social.instagram;

import net.redborder.social.util.ConfigFile;
import net.redborder.social.util.Sensor;
import net.redborder.social.util.SensorType;
import net.redborder.taskassigner.MappedTask;
import net.redborder.taskassigner.Task;
import net.redborder.taskassigner.TasksChangedListener;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by andresgomez on 29/1/15.
 */
public class InstagramManager implements TasksChangedListener {

    private List<String> runningTask;
    private Map<String, LinkedBlockingQueue<String>> msgQueue;
    private Map<String, InstagramProducer> producers;
    private Map<String, InstagramConsumer> consumers;
    private Logger l;

    public InstagramManager() {
        msgQueue = new HashMap<>();
        producers = new HashMap<>();
        consumers = new HashMap<>();
        runningTask = new ArrayList<>();
        l = Logger.getLogger(InstagramProducer.class.getName());
    }

    @Override
    public void updateTasks(List<Task> list) {
        /* Obtain instagram sensors */
        List<Sensor> tasks = ConfigFile.getInstance().getSensors(SensorType.INSTAGRAM);
        List<Sensor> reallyTask = new ArrayList<>();
         List<InstagramSensor> instagramSensors = new ArrayList<>();



        for(Task sensorName : list){
            for(Sensor task : tasks){       // For each instagram sensor
                MappedTask sensorTask = (MappedTask) sensorName;
                InstagramSensor sensor = new InstagramSensor(sensorTask.asMap());
                // If the specified name is the task's name
                if(sensor.getSensorName().equals(task.getSensorName() + "_instagram")){
                    reallyTask.add(task);       // Add the task
                }
            }
        }
        List<String> newTask = new ArrayList<>();
        List<String> taskToRemove = new ArrayList<>();

        for (Sensor sensor : reallyTask) {
            /* Instantiate an instagram sensor for each task */
            InstagramSensor instagramSensor = new InstagramSensor(sensor.asMap());
            newTask.add(instagramSensor.getUniqueId());
            instagramSensors.add(instagramSensor);
        }

        taskToRemove.addAll(runningTask);

        l.info("[Instagram] RUNNING TASK: " + runningTask);
        taskToRemove.removeAll(newTask);
        l.info("[Instagram] TASK TO REMOVE: " + taskToRemove);
        newTask.removeAll(runningTask);
        l.info("[Instagram] TASK TO ADD: " + newTask);

        runningTask.addAll(newTask);
        runningTask.removeAll(taskToRemove);

        for (InstagramSensor instagramSensor : instagramSensors) {
            /* Instantiate an instagram consumer for each task */
            if (newTask.contains( instagramSensor.getUniqueId() )) {
                InstagramConsumer consumer = new InstagramConsumer(instagramSensor, msgQueue);
                consumers.put(instagramSensor.getUniqueId(), consumer);
                consumers.get(instagramSensor.getUniqueId()).openClient(instagramSensor);

                InstagramProducer producer = new InstagramProducer(msgQueue.get(instagramSensor.getUniqueId()),
                        instagramSensor, instagramSensor.getLocationFilter());
                producers.put(instagramSensor.getUniqueId(), producer);
                producer.start();
            }

            if (taskToRemove.contains( instagramSensor.getUniqueId() )){
                InstagramConsumer consumer = consumers.get(instagramSensor.getUniqueId());
                consumer.closeClient(instagramSensor);
                consumers.remove(instagramSensor.getUniqueId());

                InstagramProducer producer = producers.get(instagramSensor.getUniqueId());
                producer.end();
                producers.remove(instagramSensor.getUniqueId());
            }

        }

        l.info("[Instagram] RUNNING TASK: " + runningTask);

    }

    public void end(){
        for (InstagramProducer producer : producers.values()) {
            producer.end();
        }
    }

    public void reload(){
        for (InstagramProducer producer : producers.values()) {
            producer.reload();
        }
    }
}
