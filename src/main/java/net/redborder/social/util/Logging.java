package net.redborder.social.util;

import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by fernando on 21/04/16.
 */
public class Logging {

    public static Logger initLogging(String name) {
        Logger logger = Logger.getLogger(name);
        logger.setLevel(Level.parse( ConfigFile.getInstance().getLogLevel().toUpperCase() ));

        ConsoleHandler handler = new ConsoleHandler();
        handler.setLevel(Level.parse( ConfigFile.getInstance().getLogLevel().toUpperCase() ));
        logger.addHandler(handler);
        logger.info("[" + name + "] Logger has been set to " + Level.parse( ConfigFile.getInstance().getLogLevel().toUpperCase() ));

        return logger;
    }

}
