package net.redborder.social;

import net.redborder.social.twitter.TwitterConsumer;
import net.redborder.social.twitter.TwitterManager;
import net.redborder.social.util.ConfigFile;
import net.redborder.social.util.SensorType;
import net.redborder.taskassigner.ZkTasksHandler;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by andresgomez on 29/12/14.
 */
public class SocialServer {
    static ZkTasksHandler tasksHandler;
    static ConfigFile config;
    static TwitterManager twitterManager;

    public static void main(String[] args) {
        try {
            ConfigFile.init();

            config = ConfigFile.getInstance();

            tasksHandler = new ZkTasksHandler(config.getZkConnect());
            List<Map<String, Object>> task = config.getSensors(SensorType.TWITTER);
            tasksHandler.setTasks(task);
            twitterManager = new TwitterManager();
            tasksHandler.addListener(twitterManager);

            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    System.out.println("Exiting...");
                    tasksHandler.end();
                    twitterManager.end();
                }
            });

            // Add signal to reload config
            Signal.handle(new Signal("HUP"), new SignalHandler() {
                public void handle(Signal signal) {
                    System.out.println("Reload received");

                    // Reload the config file
                    try {
                        ConfigFile.getInstance().reload();
                    } catch (FileNotFoundException e) {
                        Logger.getLogger(SocialServer.class.getName()).log(Level.SEVERE, "config file not found, can't reload!");
                    }

                    // Now reload the consumer and the tasks
                    List<Map<String, Object>> task = config.getSensors(SensorType.TWITTER);
                    tasksHandler.setTasks(task);
                    tasksHandler.reload();
                    twitterManager.reload();
                }
            });

        } catch (FileNotFoundException e) {
            Logger.getLogger(SocialServer.class.getName()).log(Level.SEVERE, "config file not found");

        }
    }
}
