package net.redborder.social.util.kafka;

/**
 * Created by crodriguez on 12/4/14.
 */
public interface Consumer {
    public void prepare();
    public void send(String topic, Object obj);
    public void end();
    public void reload();
}
