package net.redborder.social.instagram;

import net.redborder.social.util.SematriaSentiment;
import net.redborder.social.util.Sensor;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.jboss.netty.util.internal.StringUtil;
import org.jinstagram.Instagram;
import org.jinstagram.entity.users.feed.MediaFeed;
import org.jinstagram.entity.users.feed.MediaFeedData;
import org.jinstagram.exceptions.InstagramException;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by fernandodominguez on 29/1/15.
 */
public class InstagramConsumer extends Thread {

    private List<String> runningTask;

    private Instagram client;
    private InstagramSensor sensor;

    private SematriaSentiment semantria;


    private ObjectMapper mapper;
    private Map<String, LinkedBlockingQueue<String>> msgQueue;

    private List<List<String>> locations;
    private final static int activityPeriod = 60;
    private final static double RAD = 0.000008998719243599958;

    public InstagramConsumer(InstagramSensor sensor, Map<String, LinkedBlockingQueue<String>> msgQueue) {

        mapper = new ObjectMapper();
        runningTask = new ArrayList<>();
        this.msgQueue = msgQueue;
        this.sensor = sensor;
        semantria = null;

    }

    public void updateTasks() {

        List<String> newTask = new ArrayList<>();
        List<String> taskToRemove = new ArrayList<>();

        taskToRemove.addAll(runningTask);

        newTask.add(sensor.getUniqueId());

        System.out.println("[Instagram] RUNNING TASK: " + runningTask);
        taskToRemove.removeAll(newTask);
        System.out.println("[Instagram] TASK TO REMOVE: " + taskToRemove);
        newTask.removeAll(runningTask);
        System.out.println("[Instagram] TASK TO ADD: " + newTask);

        if (newTask.contains(sensor.getUniqueId())) {
            runningTask.add(sensor.getUniqueId());
            openClient(sensor);
        }

        System.out.println("[Instagram] RUNNING TASK: " + runningTask);

    }

    @Override
    public void run() {

        try {
            Date max = new Date();
            Calendar c = Calendar.getInstance();
            c.setTime(max);
            c.add(Calendar.SECOND, -activityPeriod);
            Date min = c.getTime();
            MediaFeed feedGeographies = null;

            for (List<String> location : this.locations) {

                if (location.size() == 2) {      // Square method

                    String[] p1Coordinates = location.get(0).split(",");
                    String[] p2Coordinates = location.get(1).split(",");

                    float mean_lng = (Float.parseFloat(p1Coordinates[0].trim()) + Float.parseFloat(p2Coordinates[0].trim())) / 2;
                    float mean_lat = (Float.parseFloat(p1Coordinates[1].trim()) + Float.parseFloat(p2Coordinates[1].trim())) / 2;

                    double r = Math.max(Math.abs(Float.parseFloat(p2Coordinates[0].trim()) - mean_lng),
                            Math.abs(Float.parseFloat(p2Coordinates[1].trim()) - mean_lat));

                    r = r / RAD;

                    feedGeographies = client.searchMedia(mean_lat,
                            mean_lng, max, min, (int) Math.round(r));

                } else if (location.size() == 1) {        // Radious method

                    String[] parts = location.get(0).split(",");

                    feedGeographies = client.searchMedia(Float.parseFloat(parts[1].trim()),
                            Float.parseFloat(parts[0].trim()), max, min, Integer.parseInt(parts[2].trim()));

                }

                List<MediaFeedData> locationsData = feedGeographies.getData();
                System.out.println("Return instagram query, result size: " + locationsData.size());
                for (MediaFeedData mediaData : locationsData) {

                    Map<String, Object> data = complexToSimple(mediaData);

                    if (semantria != null) {
                        semantria.addEvent(data);
                    }else {
                        data.put("sentiment", "unknown");
                        data.put("category", "unknown");
                        data.put("language", "unknown");

                        String json = mapper.writeValueAsString(data);
                        LinkedBlockingQueue<String> queue = msgQueue.get(sensor.getUniqueId());
                        try {
                            queue.put(json);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        msgQueue.put(sensor.getUniqueId(), queue);
                    }
                }
            }

        } catch (InstagramException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void openClient(InstagramSensor sensor) {
        this.client = new Instagram(sensor.getClientId());
        this.locations = sensor.getLocationFilter();
        msgQueue.put(sensor.getUniqueId(), new LinkedBlockingQueue<String>(100000));

        /* Ejecuta run() cada 60 segundos */
        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        exec.scheduleAtFixedRate(this, 0, activityPeriod, TimeUnit.SECONDS);
    }

    private Map<String, Object> complexToSimple(MediaFeedData data) throws IOException {

        Map<String, Object> map = new HashMap<>();
        map.put("client_latlong", data.getLocation().getLatitude() + "," + data.getLocation().getLongitude());
        map.put("user_name", data.getUser().getFullName());
        map.put("influence", "unknown");
        map.put("type", "instagram");
        if (data.getCaption() != null) {
            String msg = data.getCaption().getText();
            map.put("msg", msg);

            try {
                String hashtags = parseHashtags(msg);
                String mentions = parseMentions(msg);
                if (hashtags != null)
                    map.put("hashtags", hashtags);
                if (mentions != null)
                    map.put("mentions", mentions);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        map.put("timestamp", data.getCreatedTime());
        map.put("user_screen_name", data.getUser().getUserName());
        //map.put("followers", 0);
        //map.put("friends", 0);
        map.put("sensor_name", sensor.getSensorName());
        //map.put("src_country_code", "");
        //map.put("msg_send_from", "");
        map.put("client_id", data.getUser().getId());
        //map.put("user_msgs", 0);
        //map.put("mentions", "");
        //map.put("user_from", "");
        map.put("user_profile_img_https", data.getUser().getProfilePictureUrl());
        map.put("picture_url", data.getImages().getStandardResolution().getImageUrl());
        map.put("likes", Integer.toString(data.getLikes().getCount()));

        return map;
    }

    private String parseHashtags(String msg) {

        List<String> found = new ArrayList<>();
        int howMany = StringUtils.countMatches(msg, "#");
        if (howMany > 0) {
            int lastIndex = 0;
            for (; howMany > 0; howMany--) {
                int index = msg.indexOf("#", lastIndex);
                int indexEnd = msg.indexOf(" ", index);
                String tag;
                if (indexEnd >= 0) {
                    tag = msg.substring(index + 1, indexEnd);
                    lastIndex = indexEnd;
                } else
                    tag = msg.substring(index + 1);

                found.add(tag);
            }
            return listToString(found);
        } else {
            return null;
        }
    }

    private String parseMentions(String msg) {

        List<String> found = new ArrayList<>();
        int howMany = StringUtils.countMatches(msg, "@");
        if (howMany > 0) {
            int lastIndex = 0;
            for (; howMany > 0; howMany--) {
                int index = msg.indexOf("@", lastIndex);
                int indexEnd = msg.indexOf(" ", index);
                String tag;
                if (indexEnd >= 0) {
                    tag = msg.substring(index + 1, indexEnd);
                    lastIndex = indexEnd;
                } else
                    tag = msg.substring(index + 1);

                found.add(tag);
            }
            return listToString(found);
        } else {
            return null;
        }
    }

    private String listToString(List<String> list) {
        String converted = "";
        for (String s : list) {
            converted += s;
            converted += " ";
        }
        return converted.trim();
    }

}
