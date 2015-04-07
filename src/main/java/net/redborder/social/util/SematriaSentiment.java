package net.redborder.social.util;

import com.semantria.CallbackHandler;
import com.semantria.Session;
import com.semantria.interfaces.ICallbackHandler;
import com.semantria.mapping.Document;
import com.semantria.mapping.output.*;
import com.semantria.serializer.JsonSerializer;
import com.semantria.utils.RequestArgs;
import com.semantria.utils.ResponseArgs;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.ObjectMapper;

import javax.print.Doc;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by andresgomez on 8/1/15.
 */
public class SematriaSentiment {

    private static SematriaSentiment theInstance = null;
    private static final Object initMonitor = new Object();
    private static Session session;
    private Map<String, Map<String, Object>> events;
    private ObjectMapper mapper;

    private List<String> twitter_fail;
    private List<String> instagram_fail;


    public SematriaSentiment() {

        ConfigFile configFile = ConfigFile.getInstance();
        String key = configFile.getFromGeneral("semantria_key");
        String secret = configFile.getFromGeneral("semantria_secret");

        mapper = new ObjectMapper();
        events = new ConcurrentHashMap<>();
        twitter_fail = new ArrayList<>();
        instagram_fail = new ArrayList<>();

        if (key != null && secret != null && key.length() > 0 && secret.length() > 0) {
            session = Session.createSession(key, secret, new JsonSerializer(), true);
            session.setCallbackHandler(new CallbackHandler());
        }
    }


    public static SematriaSentiment getInstance() {
        if (theInstance == null) {
            synchronized (initMonitor) {
                try {
                    while (theInstance == null) {
                        initMonitor.wait();
                    }
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        }
        if (session != null)
            return theInstance;
        else
            return null;
    }

    public static void init() {
        synchronized (initMonitor) {
            if (theInstance == null) {
                theInstance = new SematriaSentiment();
                initMonitor.notifyAll();
            }
        }
    }

    public void addEvent(Map<String, Object> event, String jobID) {
        String uid = String.valueOf(event.hashCode());
        events.put(uid, event);
        String text = (String) event.get("msg");

        Document doc = new Document(uid, text);
        doc.setJobId(jobID);
        Integer status = session.queueDocument(doc);
        if (status != 202) {
            System.out.println("\" ID: " + uid + "\" document queued fail! Status: " + status);

            event.put("category", "unknown");
            event.put("sentiment", "neutral");
            event.put("language", "unknown");

            try {
                if (jobID.equals("twitter")) {
                    synchronized (twitter_fail) {
                        twitter_fail.add(mapper.writeValueAsString(event));
                    }
                } else if (jobID.equals("instagram")) {
                    synchronized (instagram_fail) {
                        instagram_fail.add(mapper.writeValueAsString(event));
                    }
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    public List<String> getEvents(String jobID) throws IOException {
        List<DocAnalyticData> docs = session.getProcessedDocumentsByJobId(jobID);
        List<String> eventToSend = new ArrayList<>();

        if (jobID.equals("twitter")) {
            synchronized (twitter_fail) {
                eventToSend.addAll(twitter_fail);
                twitter_fail.clear();
            }
        }
        else if (jobID.equals("instagram")) {
            synchronized (instagram_fail) {
                eventToSend.addAll(instagram_fail);
                instagram_fail.clear();
            }
        }

        for (DocAnalyticData doc : docs) {
            Map<String, Object> event = events.get(doc.getId());
            events.remove(doc.getId());
            if (event != null) {
                Float score = doc.getSentimentScore();
                if (score > 0) {
                    event.put("sentiment", "positive");
                } else if (score < 0) {
                    event.put("sentiment", "negative");
                } else {
                    event.put("sentiment", "neutral");
                }
                event.put("sentiment_value", Float.toString(score));

                List<Float> relevances = new ArrayList<>();
                List<String> categories = new ArrayList<>();


                List<DocTopic> docCategories = doc.getTopics();

                if (docCategories != null) {
                    for (DocTopic category : docCategories) {
                        relevances.add(category.getStrengthScore());
                        categories.add(category.getTitle());
                    }

                    if (categories.size() > 0)
                        event.put("category", categories.get(relevances.indexOf(Collections.max(relevances))));
                    else
                        event.put("category", "unknown");
                } else {
                    event.put("category", "unknown");
                }

                String language = doc.getLanguage();

                if (language != null)
                    event.put("language", language);
                else
                    event.put("language", "unknown");


                eventToSend.add(mapper.writeValueAsString(event));
            }
        }
        return eventToSend;
    }
}
