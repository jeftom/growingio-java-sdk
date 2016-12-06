package com.growingio.growingapi;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

/**
 * Simple interface to the GrowingIO tracking API, intended for use in
 * server-side applications. Users are encouraged to review our Javascript
 * API for reporting user events in web applications, and our Android API
 * for use in Android mobile applications.
 *
 * The Java API doesn't provide or assume any threading model, and is designed
 * such that recording events and sending them can be easily separated.
 *
 *
 */
public class GrowingAPI {
    private final Logger logger = LoggerFactory.getLogger(GrowingAPI.class);

    private final int maxMessageSize;
    private final String clientId;
    private final boolean compressed;
    private final int eventBufferSize;
    private final int awaitTime;
    private final int flushInterval;
    private final String endpoint;



    private final List<Map<String, Object>> eventArray;

    private ObjectMapper objectMapper;

    private static GrowingAPI instance = null;

    private final ThreadPoolExecutor executor;

    public static GrowingAPI apiInstance() {
        if (instance == null) {
            instance = new GrowingAPI();
        }
        return instance;
    }

    // 可以自定义配置文件名称,在classpath下
    public static GrowingAPI apiInstance(String config) {
        if (instance == null) {
            instance = new GrowingAPI(config);
        }
        return instance;
    }


    /**
     * Constructs a GrowingAPI object associated with the default configuration file name.
     */
    private GrowingAPI() {
        this("growingApi");
    }

    /**
     * Constructs a GrowingAPI object associated with the configuration file name.
     */
    private GrowingAPI(String config) {
        GrowingConfig conf = new GrowingConfig(config);
        this.endpoint = conf.baseEndPoint();
        // 设置所有需要的参数
        this.maxMessageSize = conf.maxMessageSize();
        this.clientId = conf.clientId();
        this.compressed = conf.compressed();
        this.eventBufferSize = conf.eventBufferSize();
        this.flushInterval = conf.flushInterval() * 1000;
        this.awaitTime = conf.awaitTime();

        this.eventArray = new ArrayList<Map<String, Object>>(eventBufferSize);


        this.executor = new ThreadPoolExecutor(
                conf.corePoolSize(),
                conf.maxPoolSize(),
                0, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>());

        // 初始化flush缓存数据的线程Timer
        this.executor.execute(new FlushTimer());

        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.objectMapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
    }


    public boolean trace(String eventName, Map<String, Object> commonAttrs, Map<String, Object>  properties) {
        if (eventName == null || commonAttrs == null || properties == null) {
            logger.warn("passed wrong parameter, eventName: " + eventName + ", commonAttrs: " + commonAttrs + ", properties: " + properties);
            return false;
        }

        commonAttrs.put("n", eventName);
        commonAttrs.put("e", properties);
        if (!commonAttrs.containsKey("tm")) {
            commonAttrs.put("tm", System.currentTimeMillis());
        }

        commonAttrs.put("t", "cstm");

        eventArray.add(commonAttrs);
        logger.debug("add message id: " + commonAttrs.get("id"));

        if (eventArray.size() >= eventBufferSize) {
            logger.debug("eventArray size is full");
            flush();
        }

        return true;
    }

    /**
     * 将eventArray中的数据发送出去,在拷贝数据到data中临时存储时,需要synchronized
     */
    private Future<Boolean> flush() {
        List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();

        synchronized (this.eventArray) {
            logger.debug("flush data size: " + eventArray.size());
            data.addAll(eventArray);
            eventArray.clear();
        }

        return this.executor.submit(new Submitter(data));
    }

    /**
     * gracefully 关闭GrowingAPI,保证最后数据发送出去
     */
    public void shutdownNow() {
        Future<Boolean> lastTask = flush();

        try {
            lastTask.get(awaitTime, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.error("failed to send out the last data batch");
        }
        logger.info("last eventArray size: " + eventArray.size());
        this.executor.shutdownNow();
        try {
            this.executor.awaitTermination(awaitTime, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("Failed to send out the last data batch: " + e);
        }
    }


    class Submitter implements Callable<Boolean> {
        private Logger logger = LoggerFactory.getLogger(Submitter.class);
        private List<Map<String, Object>> data = null;

        private CloseableHttpClient httpclient = HttpClients.createDefault();

        public Submitter(List<Map<String, Object>> data) {
            this.data = data;
        }

        @Override
        public Boolean call() {
            long currentTime = System.currentTimeMillis();
            try {
                logger.debug("send out data size: " + data.size());
                sendMessages(data, endpoint + "?stm=" + currentTime);
            } catch (IOException e){
                logger.error("send messages failed: " + e);
                return false;
            }
            return true;
        }

        /**
         * apply Base64 encoding followed by URL encoding
         *
         * @param dataString JSON formatted string
         * @return encoded string for <b>data</b> parameter in API call
         * @throws NullPointerException If {@code dataString} is {@code null}
         */
        private byte[] encodeDataString(String dataString) {
            try {
                return Snappy.compress(dataString.getBytes());
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException("GrowingIO library requires utf-8 support", e);
            } catch (IOException e) {
                throw new RuntimeException("Snappy encoding error", e);
            }
        }


        /**
         * Package scope for mocking purposes
         */
    /* package */
        private boolean sendData(String dataString, String endpointUrl) throws IOException {
            logger.debug("sending data: endpointUrl = " + endpointUrl + ", data = " + dataString);
            HttpPost httpPost = new HttpPost(endpointUrl);
            httpPost.setHeader("X-Client-Id", clientId);

            httpPost.setHeader("charset", "utf-8");

            if (compressed) {
                httpPost.setHeader("Content-Type", "text/plain");
                byte[] encodedData = encodeDataString(dataString);
                httpPost.setEntity(new ByteArrayEntity(encodedData));
            } else {
                httpPost.setHeader("Content-Type", "application/json");
                httpPost.setEntity(new StringEntity(dataString));
            }

            CloseableHttpResponse response = httpclient.execute(httpPost);

            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                logger.debug("response code: 200 OK");
                try {
                    logger.debug(EntityUtils.toString(response.getEntity()));
                    return true;
                } finally {
                    response.close();
                }
            }
            return false;
        }

        private void sendMessages(List<Map<String, Object>> messages, String endpointUrl) throws IOException {
            for (int i = 0; i < messages.size(); i += maxMessageSize) {
                int endIndex = i + maxMessageSize;
                endIndex = Math.min(endIndex, messages.size());
                List<Map<String, Object>> batch = messages.subList(i, endIndex);

                if (batch.size() > 0) {
                    String messagesString = objectMapper.writeValueAsString(batch);
                    boolean accepted = sendData(messagesString, endpointUrl);

                    if (! accepted) {
                        for (Map<String, Object> m: batch) {
                            logger.warn("failed to send data: " + m);
                        }
                    }
                }
            }
        }
    }

    /**
     * 定时将eventArray中的数据发送出去
     */
    class FlushTimer implements Runnable {

        @Override
        public void run() {
            while (!Thread.interrupted()) {
                try {
                    if (!executor.isShutdown()) {
                        flush();
                    }
                    Thread.sleep(flushInterval);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.info("close the FlushTimer thread. ");
                }
            }
        }
    }

}