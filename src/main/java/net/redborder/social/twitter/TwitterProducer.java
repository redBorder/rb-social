package net.redborder.social.twitter;

import com.semantria.CallbackHandler;
import com.semantria.Session;
import com.semantria.interfaces.ICallbackHandler;
import com.semantria.mapping.Document;
import com.semantria.mapping.output.CollAnalyticData;
import com.semantria.mapping.output.DocAnalyticData;
import com.semantria.utils.RequestArgs;
import com.semantria.utils.ResponseArgs;
import com.twitter.hbc.core.endpoint.Location;
import net.redborder.social.util.ConfigFile;
import net.redborder.social.util.Logging;
import net.redborder.social.util.SematriaSentiment;
import net.redborder.social.util.Sensor;
import net.redborder.social.util.kafka.KafkaProducer;
import net.redborder.social.util.kafka.ZkKafkaBrokers;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by andresgomez on 30/12/14.
 */
public class TwitterProducer extends Thread {
    private KafkaProducer producer;
    BlockingQueue<String> msgQueue;
    ObjectMapper mapper;
    String sensorName;
    Map<String, Object> enrichment;
    SematriaSentiment semantria;
    List<List<String>> locations;

    private Logger logger;

    private final int SLEEP_DELAY = 1500;

    public TwitterProducer(BlockingQueue<String> msgQueue, Sensor sensor, List<List<String>> locations) {
        producer = new KafkaProducer(new ZkKafkaBrokers());
        producer.prepare();
        this.msgQueue = msgQueue;
        this.sensorName = sensor.getSensorName();
        this.enrichment = sensor.getEnrichment();
        mapper = new ObjectMapper();
        SematriaSentiment.init();
        semantria = SematriaSentiment.getInstance();
        this.locations = locations;
        this.logger = Logging.initLogging(this.getClass().getName());
    }

    @Override
    public void run() {
        while (true) {
            while (!msgQueue.isEmpty()) {
                String msg = null;
                Map<String, Object> data = null;
                try {
                    msg = msgQueue.take();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                try {
                    data = complexToSimple(msg);

                    if (data != null) {
                        if (semantria != null)
                            semantria.addEvent((Map<String, Object>) data.get("tweet"), "twitter");

                        List<Map<String, Object>> hashTags = (List<Map<String, Object>>) data.get("hashtags");

                        if (!hashTags.isEmpty()) {
                            List<String> hashtagStr = hashTagsParser(hashTags);
                            for (String hash : hashtagStr)
                                producer.send("rb_hashtag", hash);
                        } else {
                            logger.fine("No hashtags found for Tweet " + data);
                        }

                        List<Map<String, Object>> url = (List<Map<String, Object>>) data.get("urls");

                        if (!url.isEmpty()) {
                            List<String> urlStr = urlsParser(url);
                            for (String urlS : urlStr)
                                producer.send("rb_hashtag", urlS);
                        } else {
                            logger.fine("No URLs found for Tweet " + data);
                        }

                        List<Map<String, Object>> mentions = (List<Map<String, Object>>) data.get("user_mentions");

                        if (!mentions.isEmpty()) {
                            List<String> mentionStr = mentionsParser(mentions);
                            for (String mention : mentionStr)
                                producer.send("rb_hashtag", mention);
                        } else {
                            logger.fine("No mentions found for " + data);
                        }

                        if (semantria != null) {
                            List<String> msgs = semantria.getEvents("twitter");
                            for (String msgToSend : msgs)
                                producer.send("rb_social", msgToSend);
                        } else {
                            producer.send("rb_social", data.get("tweet"));
                            logger.fine("Semantria is disabled. Skipping sentiment enrichment.");
                        }
                    } else {
                        logger.warning("Could not parse tweet: " + msg);
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            logger.fine("Msq queue is empty. Sleeping for " + SLEEP_DELAY + " secs");
            try {
                Thread.sleep(SLEEP_DELAY);
            } catch (InterruptedException e) {
                logger.warning("Was interrupted while consuming messages");
                e.printStackTrace();
            }
        }
    }

    public void end() {
        producer.end();
    }

    public void reload() {
        producer.reload();
    }


    public Map<String, Object> complexToSimple(String tweet) throws IOException {
        Map<String, Object> complexTweet = (Map<String, Object>) mapper.readValue(tweet, Map.class);
        Map<String, Object> simpleTweet = new HashMap<>();

        simpleTweet.put("type", "twitter");

        String timestampStr = (String) complexTweet.get("timestamp_ms");

        if (timestampStr != null) {
            Long timestamp = Long.valueOf(timestampStr);
            simpleTweet.put("timestamp", timestamp / 1000);
        } else {
            simpleTweet.put("timestamp", System.currentTimeMillis() / 1000);
        }

        String msg = (String) complexTweet.get("text");

        if (msg != null)
            simpleTweet.put("msg", msg);
        else
            logger.warning("Could not get Tweet message from " + msg);

        Map<String, Object> user = (Map<String, Object>) complexTweet.get("user");

        if (user != null) {
            String id = (String) user.get("id_str");
            String name = (String) user.get("name");
            String username = (String) user.get("screen_name");
            Integer followers = (Integer) user.get("followers_count");
            Integer friends = (Integer) user.get("friends_count");
            Integer tweets = (Integer) user.get("statuses_count");
            String language = (String) user.get("es");
            String image_url = (String) user.get("profile_image_url_https");
            String from = (String) user.get("location");

            if (followers > 10000000) {
                simpleTweet.put("influence", "extremly high");
            } else if (followers > 1000000) {
                simpleTweet.put("influence", "very high");
            } else if (followers > 500000) {
                simpleTweet.put("influence", "high");
            } else if (followers > 10000) {
                simpleTweet.put("influence", "medium");
            } else if (followers > 500) {
                simpleTweet.put("influence", "low");
            } else {
                if (tweets > 1500) {
                    simpleTweet.put("influence", "low");
                } else {
                    simpleTweet.put("influence", "very low");
                }
            }

            if (id != null && id.length() > 0)
                simpleTweet.put("client_id", id);
            if (name != null && name.length() > 0)
                simpleTweet.put("user_name", name);
            if (username != null && username.length() > 0)
                simpleTweet.put("user_screen_name", username);
            if (followers != null)
                simpleTweet.put("followers", followers);
            if (friends != null)
                simpleTweet.put("friends", friends);
            if (tweets != null)
                simpleTweet.put("user_msgs", tweets);
            if (language != null && language.length() > 0)
                simpleTweet.put("user_language", language);
            if (image_url != null && image_url.length() > 0)
                simpleTweet.put("user_profile_img_https", image_url);
            if (from != null && from.length() > 0)
                simpleTweet.put("user_from", from);
        }

        Map<String, Object> geo = (Map<String, Object>) complexTweet.get("geo");
        Map<String, Object> coordinates = (Map<String, Object>) complexTweet.get("coordinates");


        boolean intoSquare = false;

        Double lat = 0.00;
        Double lon = 0.00;

        if (geo != null) {
            List<Number> coord = (ArrayList<Number>) geo.get("coordinates");
            lat = coord.get(0).doubleValue();
            lon = coord.get(1).doubleValue();
        } else if (coordinates != null) {
            List<Number> coord = (ArrayList<Number>) coordinates.get("coordinates");
            lat = coord.get(1).doubleValue();
            lon = coord.get(0).doubleValue();
        } else {
            intoSquare = true;
        }

        if (!intoSquare) {
            for (List<String> location : locations) {
                String[] longLatSouthWest = location.get(0).split(",");
                String[] longLatNorthEast = location.get(1).split(",");

                if (lat != 0 && lon != 0) {
                    if (lat > Double.valueOf(longLatSouthWest[1].trim()) && lat < Double.valueOf(longLatNorthEast[1].trim()) && lon > Double.valueOf(longLatSouthWest[0].trim()) && lon < Double.valueOf(longLatNorthEast[0].trim())) {
                        simpleTweet.put("client_latlong", lat + "," + lon);
                        intoSquare = true;
                        break;
                    }
                }
            }
        }

        Map<String, Object> place = (Map<String, Object>) complexTweet.get("place");

        if (place != null) {
            String code = (String) place.get("country_code");
            String tweet_loc = (String) place.get("full_name");

            if (code != null)
                simpleTweet.put("src_country_code", code);
            if (tweet_loc != null)
                simpleTweet.put("msg_send_from", tweet_loc);
        }

      /*  Integer retweet_count = (Integer) complexTweet.get("retweet_count");
        Integer favorite_count = (Integer) complexTweet.get("favorite_count");

        if (retweet_count != null)
            simpleTweet.put("msg_share_count", retweet_count);
        if (favorite_count != null)
            simpleTweet.put("msg_favorite_count", favorite_count); */
        Map<String, Object> data = new HashMap<>();

        Map<String, Object> entities = (Map<String, Object>) complexTweet.get("entities");

        if (entities != null) {

            List<Map<String, Object>> hashtags = (ArrayList<Map<String, Object>>) entities.get("hashtags");
            List<Map<String, Object>> urls = (ArrayList<Map<String, Object>>) entities.get("urls");
            List<Map<String, Object>> user_mentions = (ArrayList<Map<String, Object>>) entities.get("user_mentions");

            simpleTweet.put("sensor_name", sensorName);
            simpleTweet.putAll(enrichment);

            String hashtagStr = " ";
            for (Map<String, Object> hashtag : hashtags) {
                if (!hashtag.isEmpty()) {
                    String text = (String) hashtag.get("text");
                    hashtagStr = hashtagStr + " #" + text;
                }
            }

            hashtagStr = hashtagStr + " ";

            if (hashtagStr.length() > 2)
                simpleTweet.put("hashtags", hashtagStr);

            String urlsList = " ";
            for (Map<String, Object> url : urls) {
                if (!url.isEmpty()) {
                    String text = (String) url.get("expanded_url");
                    urlsList = urlsList + " " + text;
                }
            }

            urlsList = urlsList + " ";

            if (urlsList.length() > 2)
                simpleTweet.put("urls", urlsList);

            String mentionsList = " ";
            for (Map<String, Object> user_mention : user_mentions) {
                if (!user_mention.isEmpty()) {
                    String text = (String) user_mention.get("screen_name");
                    if (!mentionsList.contains(text))
                        mentionsList = mentionsList + " @" + text;
                }
            }

            mentionsList = mentionsList + " ";

            if (mentionsList.length() > 2)
                simpleTweet.put("mentions", mentionsList);


            data.put("hashtags", hashtags);
            data.put("urls", urls);
            data.put("user_mentions", user_mentions);
        }

        if (semantria != null)
            data.put("tweet", simpleTweet);
        else {
            simpleTweet.put("sentiment", "unknown");
            simpleTweet.put("category", "unknown");
            simpleTweet.put("language", "unknown");
            data.put("tweet", mapper.writeValueAsString(simpleTweet));
        }

        if (intoSquare)
            return data;
        else
            return null;
    }

    public List<String> hashTagsParser(List<Map<String, Object>> hashtags) throws IOException {
        List<String> hashtagsList = new ArrayList<>();
        for (Map<String, Object> hashtag : hashtags) {
            if (!hashtag.isEmpty()) {
                Map<String, Object> hashCount = new HashMap<>();
                String text = (String) hashtag.get("text");
                hashCount.put("value", "#" + text);
                hashCount.put("type", "hashtag");
                hashCount.put("sensor_name", sensorName);
                hashCount.putAll(enrichment);
                hashCount.put("timestamp", System.currentTimeMillis() / 1000);
                hashtagsList.add(mapper.writeValueAsString(hashCount));
            }
        }
        return hashtagsList;
    }

    public List<String> urlsParser(List<Map<String, Object>> urls) throws IOException {
        List<String> urlsList = new ArrayList<>();
        for (Map<String, Object> url : urls) {
            if (!url.isEmpty()) {
                Map<String, Object> urlCount = new HashMap<>();
                String text = (String) url.get("expanded_url");
                urlCount.put("value", text);
                urlCount.put("type", "url");
                urlCount.put("sensor_name", sensorName);
                urlCount.putAll(enrichment);
                urlCount.put("timestamp", System.currentTimeMillis() / 1000);
                urlsList.add(mapper.writeValueAsString(urlCount));
            }
        }
        return urlsList;
    }

    public List<String> mentionsParser(List<Map<String, Object>> mentions) throws IOException {
        List<String> mentionsList = new ArrayList<>();
        for (Map<String, Object> user_mention : mentions) {
            if (!user_mention.isEmpty()) {
                Map<String, Object> user_mentionCount = new HashMap<>();
                String text = (String) user_mention.get("screen_name");
                user_mentionCount.put("value", "@" + text);
                user_mentionCount.put("type", "user_mention");
                user_mentionCount.put("sensor_name", sensorName);
                user_mentionCount.putAll(enrichment);
                user_mentionCount.put("timestamp", System.currentTimeMillis() / 1000);
                mentionsList.add(mapper.writeValueAsString(user_mentionCount));
            }
        }
        return mentionsList;
    }
}
