package net.redborder.social.util;

import com.semantria.Session;
import com.semantria.interfaces.ICallbackHandler;
import com.semantria.mapping.Document;
import com.semantria.mapping.output.*;
import com.semantria.utils.RequestArgs;
import com.semantria.utils.ResponseArgs;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.ObjectMapper;

import javax.print.Doc;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

/**
 * Created by andresgomez on 8/1/15.
 */
public class SematriaSentiment {

    private static SematriaSentiment theInstance = null;
    private static final Object initMonitor = new Object();
    private static Session session;
    private Map<String, Map<String, Object>> events;
    private ObjectMapper mapper;


    public SematriaSentiment() {

        ConfigFile configFile = ConfigFile.getInstance();
        String key = configFile.getFromGeneral("semantria_key");
        String secret = configFile.getFromGeneral("semantria_secret");

        mapper = new ObjectMapper();
        events = new HashMap<>();

        if (key != null && secret != null && key.length() > 0 && secret.length() > 0) {
            session = Session.createSession(key, secret, true);
            session.setCallbackHandler(new ICallbackHandler() {
                @Override
                public void onResponse(Object o, ResponseArgs responseArgs) {

                }

                @Override
                public void onRequest(Object o, RequestArgs requestArgs) {

                }

                @Override
                public void onError(Object o, ResponseArgs responseArgs) {

                }

                @Override
                public void onDocsAutoResponse(Object o, List<DocAnalyticData> list) {

                }

                @Override
                public void onCollsAutoResponse(Object o, List<CollAnalyticData> list) {

                }
            });
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

    public void addEvent(Map<String, Object> event) {
        String uid = String.valueOf(event.hashCode());
        events.put(uid, event);
        String text = (String) event.get("msg");

        if (session.queueDocument(new Document(uid, text)) != 202) {
            System.out.println("\"" + uid + "\" document queued fail!");
        }
    }

    public List<String> getEvents() throws IOException {
        List<DocAnalyticData> docs = session.getProcessedDocuments();
        List<String> eventToSend = new ArrayList<>();
        for (DocAnalyticData doc : docs) {
            Map<String, Object> event = events.get(doc.getId());
            events.remove(doc.getId());
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
                    event.put("category", categories.get(Collections.max(relevances).intValue()));
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
        return eventToSend;
    }
}
