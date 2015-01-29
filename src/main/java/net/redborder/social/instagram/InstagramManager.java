package net.redborder.social.instagram;

import net.redborder.social.twitter.TwitterConsumer;
import net.redborder.social.twitter.TwitterProducer;
import net.redborder.social.twitter.TwitterSensor;
import net.redborder.social.util.ConfigFile;
import net.redborder.social.util.Sensor;
import net.redborder.social.util.SensorType;
import net.redborder.taskassigner.MappedTask;
import net.redborder.taskassigner.Task;
import net.redborder.taskassigner.TasksChangedListener;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by andresgomez on 29/1/15.
 */
public class InstagramManager implements TasksChangedListener {

    List<String> runningTask;
    private Map<String, LinkedBlockingQueue<String>> msgQueue;
    private Map<String, InstagramProducer> producers;
    private Map<String, InstagramConsumer> consumers;


    public InstagramManager() {
        msgQueue = new HashMap<>();
        producers = new HashMap<>();
        consumers = new HashMap<>();
        runningTask = new ArrayList<>();
    }

    @Override
    public void updateTasks(List<Task> list) {
        List<Sensor> tasks = ConfigFile.getInstance().getSensors(SensorType.INSTAGRAM);
        List<Sensor> reallyTask = new ArrayList<>();

        for(Task sensorName : list){
            for(Sensor task : tasks){
                MappedTask sensorTask = (MappedTask) sensorName;
                InstagramSensor instagramSensor = new InstagramSensor(sensorTask.asMap());
                if(instagramSensor.getSensorName().equals(task.getSensorName() + "_instagram")){
                    reallyTask.add(task);
                }
            }
        }
    }
}
