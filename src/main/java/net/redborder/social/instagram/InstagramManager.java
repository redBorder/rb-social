package net.redborder.social.instagram;

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

    private List<String> runningTask;
    private List<InstagramSensor> instagramSensors;
    private Map<String, LinkedBlockingQueue<String>> msgQueue;
    private Map<String, InstagramProducer> producers;
    private Map<String, InstagramConsumer> consumers;

    private InstagramConsumer consumer;


    public InstagramManager() {
        msgQueue = new HashMap<>();
        producers = new HashMap<>();
        consumers = new HashMap<>();
        runningTask = new ArrayList<>();
        instagramSensors = new ArrayList<>();
    }

    @Override
    public void updateTasks(List<Task> list) {
        /* Obtiene los sensores Instagram */
        List<Sensor> tasks = ConfigFile.getInstance().getSensors(SensorType.INSTAGRAM);
        List<Sensor> reallyTask = new ArrayList<>();

        for(Task sensorName : list){
            for(Sensor task : tasks){       // Para cada sensor instagram
                MappedTask sensorTask = (MappedTask) sensorName;
                InstagramSensor instagramSensor = new InstagramSensor(sensorTask.asMap());
                instagramSensors.add(instagramSensor);
                // Si el nombre del sensor es el especificado en la tarea
                if(instagramSensor.getSensorName().equals(task.getSensorName() + "_instagram")){
                    reallyTask.add(task);       // Añadimos la tarea
                }
            }
        }


        for (Sensor sensor : reallyTask) {
            InstagramSensor instagramSensor = (InstagramSensor) sensor;
            consumer = new InstagramConsumer(instagramSensor, msgQueue);
            consumer.updateTasks();
        }

        List<String> newTask = new ArrayList<>();
        List<String> taskToRemove = new ArrayList<>();
        List<InstagramSensor> instagramSensors = new ArrayList<>();

        taskToRemove.addAll(runningTask);

        for (Sensor sensor : reallyTask) {
            InstagramSensor instagramSensor = (InstagramSensor) sensor;
            newTask.add(instagramSensor.getUniqueId());
            instagramSensors.add(instagramSensor);
        }

        taskToRemove.removeAll(newTask);
        newTask.removeAll(runningTask);

        runningTask.addAll(newTask);
        runningTask.removeAll(taskToRemove);

        for (InstagramSensor instagramSensor : instagramSensors) {

            if (newTask.contains(instagramSensor.getUniqueId())) {
                InstagramProducer producer = new InstagramProducer(msgQueue.get(instagramSensor.getUniqueId()),
                        instagramSensor.getSensorName(), instagramSensor.getLocationFilter());
                producers.put(instagramSensor.getSensorName(), producer);
                producer.start();
            }

            if (taskToRemove.contains(instagramSensor.getUniqueId())) {
                InstagramProducer producer = producers.get(instagramSensor.getSensorName());
                producer.end();
                producers.remove(instagramSensor.getSensorName());
            }

        }
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
