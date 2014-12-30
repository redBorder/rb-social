package net.redborder.social;

import net.redborder.social.twitter.TwitterConsumer;
import net.redborder.social.twitter.TwitterManager;
import net.redborder.social.util.ConfigFile;
import net.redborder.social.util.SensorType;
import net.redborder.taskassigner.ZkTasksHandler;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.util.List;
import java.util.Map;

/**
 * Created by andresgomez on 29/12/14.
 */
public class SocialServer {
    static ZkTasksHandler tasksHandler;
    static ConfigFile config;
    static TwitterManager twitterManager;

    public static void main(String[] args) {
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
                ConfigFile.getInstance().reload();

                // Now reload the consumer and the tasks
                List<Map<String, Object>> task = config.getSensors(SensorType.TWITTER);
                tasksHandler.setTasks(task);
                tasksHandler.reload();
                twitterManager.reload();
            }
        });
    }
}
