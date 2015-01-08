package net.redborder.social.twitter;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.Location;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import net.redborder.social.util.Sensor;
import net.redborder.taskassigner.TasksChangedListener;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by andresgomez on 29/12/14.
 */
public class TwitterConsumer {

    private static Map<String, LinkedBlockingQueue<String>> msgQueue;
    private static Map<String, LinkedBlockingQueue<Event>> eventQueue;

    private List<String> runningTask;
    private Map<String, Client> runningHbc;


    public TwitterConsumer(Map<String, LinkedBlockingQueue<String>> msgQueue) {
        runningTask = new ArrayList<>();
        runningHbc = new HashMap<>();
        this.msgQueue = msgQueue;
        eventQueue = new HashMap<>();
    }

    public void updateTasks(List<Sensor> list) {

        List<String> newTask = new ArrayList<>();
        List<String> taskToRemove = new ArrayList<>();
        List<TwitterSensor> twitterSensors = new ArrayList<>();

        taskToRemove.addAll(runningTask);


        for (Sensor sensor : list) {
            TwitterSensor twitterSensor = (TwitterSensor) sensor;
            newTask.add(twitterSensor.getConsumerKey());
            twitterSensors.add(twitterSensor);
        }

        System.out.println("RUNNING TASK: " + runningTask);
        taskToRemove.removeAll(newTask);
        System.out.println("TASK TO REMOVE: " + taskToRemove);
        newTask.removeAll(runningTask);
        System.out.println("TASK TO ADD: " + newTask);

        for (TwitterSensor twitterSensor : twitterSensors) {
            if (newTask.contains(twitterSensor.getConsumerKey())) {
                runningTask.add(twitterSensor.getConsumerKey());
                openClient(twitterSensor);
            }
        }

        for(String task : taskToRemove){
            runningTask.remove(task);
            closeClient(task);
        }

        System.out.println("RUNNING TASK: " + runningTask);
    }

    public void closeClient(String task){
        Client client = runningHbc.get(task);
        msgQueue.remove(client.getName());
        runningHbc.remove(task);
        client.stop();
    }

    public void openClient(TwitterSensor sensor) {
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();

        msgQueue.put(sensor.getSensorName(), new LinkedBlockingQueue<String>(100000));
        eventQueue.put(sensor.getSensorName(), new LinkedBlockingQueue<Event>(10000));

        if (!sensor.getTextFilters().isEmpty())
            endpoint.trackTerms(sensor.getTextFilters());

        if (!sensor.getLocationFilters().isEmpty()) {
            List<Location> locations = new ArrayList<>();

            for (List<String> location : sensor.getLocationFilters()) {
                String[] longLatSouthWest = location.get(0).split(",");
                Location.Coordinate southwest = new Location.Coordinate(Double.valueOf(longLatSouthWest[0].trim()), Double.valueOf(longLatSouthWest[1].trim()));
                String[] longLatNorthEast = location.get(1).split(",");
                Location.Coordinate northeast = new Location.Coordinate(Double.valueOf(longLatNorthEast[0].trim()), Double.valueOf(longLatNorthEast[1].trim()));
                Location loc = new Location(southwest, northeast);
                locations.add(loc);
            }

            endpoint.locations(locations);
        }

        Authentication hosebirdAuth = new OAuth1(sensor.getConsumerKey(),
                sensor.getConsumerSecret(),
                sensor.getTokenKey(),
                sensor.getTokenSecret());

        ClientBuilder builder = new ClientBuilder()
                .name(sensor.getSensorName())
                .hosts(hosebirdHosts)
                .endpoint(endpoint)
                .authentication(hosebirdAuth)
                .processor(new StringDelimitedProcessor(msgQueue.get(sensor.getSensorName())))
                .eventMessageQueue(eventQueue.get(sensor.getSensorName()));

        Client hbc = builder.build();

        hbc.connect();
        runningHbc.put(sensor.getConsumerKey(), hbc);
    }

    public void end(){
        for(Client c :runningHbc.values())
            c.stop();
    }
}
