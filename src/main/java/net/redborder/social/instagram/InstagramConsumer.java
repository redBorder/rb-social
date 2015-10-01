package net.redborder.social.instagram;

import net.redborder.social.util.SematriaSentiment;
import net.redborder.social.util.kafka.KafkaProducer;
import net.redborder.social.util.kafka.ZkKafkaBrokers;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.jboss.netty.util.internal.StringUtil;
import org.jinstagram.Instagram;
import org.jinstagram.entity.users.feed.MediaFeed;
import org.jinstagram.entity.users.feed.MediaFeedData;
import org.jinstagram.exceptions.InstagramException;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;


/**
 * Created by fernandodominguez on 29/1/15.
 */
public class InstagramConsumer extends Thread {

    private Instagram client;
    private InstagramSensor sensor;

    private SematriaSentiment semantria;

    ScheduledFuture thread;

    private ObjectMapper mapper;
    private Map<String, LinkedBlockingQueue<String>> msgQueue;

    private List<List<String>> locations;
    private final static int activityPeriod = 60;
    private final static double RAD = 0.000008998719243599958;
    private KafkaProducer kafkaProducer;

    private Logger l;

    public InstagramConsumer(InstagramSensor sensor, Map<String, LinkedBlockingQueue<String>> msgQueue) {

        mapper = new ObjectMapper();
        this.msgQueue = msgQueue;
        this.sensor = sensor;
        l = Logger.getLogger(InstagramProducer.class.getName());
        semantria = SematriaSentiment.getInstance();
        kafkaProducer = new KafkaProducer(new ZkKafkaBrokers());
        kafkaProducer.prepare();

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

                    l.debug("COORDINATES [lat: " + mean_lat + " long:" + mean_lng + " r: " + r + "]");
                    feedGeographies = client.searchMedia(mean_lat,
                            mean_lng, max, min, (int) Math.round(r));

                } else if (location.size() == 1) {        // Radious method

                    String[] parts = location.get(0).split(",");

                    feedGeographies = client.searchMedia(Float.parseFloat(parts[1].trim()),
                            Float.parseFloat(parts[0].trim()), max, min, Integer.parseInt(parts[2].trim()));

                }

                List<MediaFeedData> locationsData = feedGeographies.getData();
                l.debug("Return instagram query from " + sensor.getSensorName() + ", result size: " + locationsData.size());
                for (MediaFeedData mediaData : locationsData) {

                    Map<String, Object> data = complexToSimple(mediaData);

                    if (semantria != null) {
                        semantria.addEvent(data, "instagram");
                    } else {
                        data.put("sentiment", "unknown");
                        data.put("category", "unknown");
                        data.put("language", "unknown");

                        String json = mapper.writeValueAsString(data);
                        l.debug("Added msg to queueId: " + sensor.getUniqueId() + "\n" + json);
                        msgQueue.get(sensor.getUniqueId()).put(json);

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

    protected void openClient(InstagramSensor sensor) {
        this.client = new Instagram(sensor.getClientId());
        this.locations = sensor.getLocationFilter();
        msgQueue.put(sensor.getUniqueId(), new LinkedBlockingQueue<String>(100000));

        /* Executes run() each activityPeriod */
        thread = Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(this, 0, activityPeriod, TimeUnit.SECONDS);
    }


    protected void closeClient(InstagramSensor sensor) {
        thread.cancel(true);
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
                List<String> hashtags = parseHashtags(msg);
                List<String> mentions = parseMentions(msg);
                if (hashtags != null)
                    map.put("hashtags", listToString(hashtags));
                if (mentions != null)
                    map.put("mentions", listToString(mentions));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        map.put("timestamp", data.getCreatedTime());
        map.put("user_screen_name", data.getUser().getUserName());
        //map.put("followers", 0);
        //map.put("friends", 0);
        map.put("sensor_name", sensor.getSensorName());
        map.putAll(sensor.getEnrichment());

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

    private List<String> parseHashtags(String msg) {

        List<String> found = new ArrayList<>();
        int howMany = StringUtils.countMatches(msg, "#");
        if (howMany > 0) {
            int lastIndex = 0;
            for (; howMany > 0; howMany--) {
                int index = msg.indexOf("#", lastIndex);
                int indexEnd = msg.indexOf(" ", index);
                String tag;
                if (indexEnd >= 0) {
                    tag = msg.substring(index, indexEnd);
                    lastIndex = indexEnd;
                } else
                    tag = msg.substring(index);

                found.add(tag);
            }
            return found;
        } else {
            return null;
        }
    }

    private List<String> parseMentions(String msg) {

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
            return found;
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
