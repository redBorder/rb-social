package net.redborder.social;

import net.redborder.social.instagram.InstagramManager;
import net.redborder.social.twitter.TwitterManager;
import net.redborder.social.util.ConfigFile;
import net.redborder.social.util.Sensor;
import net.redborder.social.util.SensorType;
import net.redborder.taskassigner.Task;
import net.redborder.taskassigner.ZkTasksHandler;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by andresgomez on 29/12/14.
 */
public class SocialServer {
    static ZkTasksHandler tasksHandler;
    static TwitterManager twitterManager;
    static InstagramManager instagramManager;
    static Object running;

    public static void main(String[] args) {
        try {
            ConfigFile.init();
            running = new Object();
            List<Task> tasks = new ArrayList<>();

            tasksHandler = new ZkTasksHandler(ConfigFile.getInstance().getZkConnect(), "/rb-social");
            List<Sensor> sensors = ConfigFile.getInstance().getSensorNames(SensorType.TWITTER);

            for(Sensor s : sensors){
                tasks.add(s);
            }

            final List<Sensor> instagramSensors = ConfigFile.getInstance().getSensorNames(SensorType.INSTAGRAM);

            for(Sensor s : instagramSensors){
                tasks.add(s);
            }

            tasksHandler.setTasks(tasks);

            twitterManager = new TwitterManager();
            instagramManager = new InstagramManager();

            tasksHandler.addListener(twitterManager);
            tasksHandler.addListener(instagramManager);

            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    System.out.println("Exiting...");
                    tasksHandler.end();
                    twitterManager.end();
                    instagramManager.end();
                    synchronized (running) {
                        running.notifyAll();
                    }
                }
            });

            // Add signal to reload config
            Signal.handle(new Signal("HUP"), new SignalHandler() {
                public void handle(Signal signal) {
                    System.out.println("Reload received!");
                    // Reload the config file
                    try {
                        ConfigFile.getInstance().reload();
                    } catch (FileNotFoundException e) {
                        Logger.getLogger(SocialServer.class.getName()).log(Level.SEVERE, "config file not found, can't reload!");
                    }

                    // Now reload the consumer and the tasks
                    List<Sensor> sensors = ConfigFile.getInstance().getSensorNames(SensorType.TWITTER);
                    List<Task> tasks = new ArrayList<>();

                    for(Sensor s : sensors){
                        tasks.add(s);
                    }

                    tasksHandler.setTasks(tasks);
                    twitterManager.reload();
                    instagramManager.reload();
                    tasksHandler.reload();
                    System.out.println("Reload finished!");
                }
            });

            synchronized (running) {
                running.wait();
            }

        } catch (FileNotFoundException e) {
            Logger.getLogger(SocialServer.class.getName()).log(Level.SEVERE, "config file not found");
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
