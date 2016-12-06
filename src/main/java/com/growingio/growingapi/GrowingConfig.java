package com.growingio.growingapi;

import com.typesafe.config.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by king on 7/11/16.
 */
public class GrowingConfig {
    private static Logger logger = LoggerFactory.getLogger(GrowingConfig.class);

//    private static String CONFIG_NAME = "growingApi";
    private Config config;

    public GrowingConfig(String config) {
        this.config = ConfigFactory.load(config);

        validation();
    }

    public String baseEndPoint() {
        String endPoint = null;
        try {
            endPoint = config.getString("app.base_end_point") + "/custom/" + config.getString("app.ai") + "/events";
        } catch (Exception e) {
            logger.info("did not find base_end_point configuration");
            if (isDebug()) {
                endPoint = "http://elb-api-pt-1996323097.cn-north-1.elb.amazonaws.com.cn" +
                        "/custom/" + config.getString("app.ai") + "/events";
            } else {
                endPoint = "https://api.growingio.com" +
                        "/custom/" + config.getString("app.ai") + "/events";
            }
            logger.info("using end_point: " + endPoint);
        }
        return endPoint;
    }

    public Boolean isDebug() {
        boolean isDebug = false;
        try {
            isDebug = config.getString("app.mode").equals("debug");
        } catch (Exception e) {
            logger.info("use default production mode");
        }
        return isDebug;
    }

    public int maxMessageSize() {
        int maxMessageSize = 50;
        try {
            maxMessageSize = config.getInt("app.max_message_size");
        } catch (Exception e) {
            logger.info("use default maxMessageSize: 50");
        }
        return maxMessageSize;
    }

    public String clientId() {
        return config.getString("app.client_id");
    }

    public int corePoolSize() {
        int corePoolSize = 3;
        try {
            corePoolSize = config.getInt("app.core_pool_size");
        } catch (Exception e) {
            logger.info("use default corePoolSize: 3");
        }
        return corePoolSize;
    }

    public int maxPoolSize() {
        int maxPoolSize = 4;
        try {
            maxPoolSize = config.getInt("app.max_pool_size");
        } catch (Exception e) {
            logger.info("use default maxPoolSize: 4");
        }
        return maxPoolSize;
    }

    public int eventBufferSize() {
        int eventBufferSize = 200;
        try {
            eventBufferSize = config.getInt("app.event_buffer_size");
        } catch (Exception e) {
            logger.info("use default eventBufferSize: 200");
        }
        return eventBufferSize;
    }

    /**
     * flush timer interval, TimeUnit = Seconds
     * @return flush interval
     */
    public int flushInterval() {
        int flushInterval = 5;
        try {
            flushInterval = config.getInt("app.flush_interval");
        } catch (Exception e) {
            logger.info("use default flushInterval: 4");
        }
        return flushInterval;
    }

    public int awaitTime() {
        int awaitTime = 30;
        try {
            awaitTime = config.getInt("app.await_time");
        } catch (Exception e) {
            logger.info("use default awaitTime: 30");
        }
        return awaitTime;
    }


    public boolean compressed() {
        boolean compressed = true;
        try {
            compressed = config.getBoolean("app.compressed");
        } catch (Exception e) {
            logger.info("used default none compress");
        }
        return compressed;
    }


    @Override
    public String toString() {
        return config.root().render(ConfigRenderOptions.concise());
    }

    /**
     * validate the necessary properties here
     * app.ai, app.clientId
     */
    private void validation() {
        try {
            config.getString("app.ai");
            config.getString("app.client_id");
        } catch (Exception e) {
            throw new RuntimeException("Missing configuration for app.ai and app.client_id");
        }
    }

}
