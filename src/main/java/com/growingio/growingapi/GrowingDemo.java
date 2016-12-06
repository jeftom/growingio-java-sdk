package com.growingio.growingapi;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by king on 7/11/16.
 */
public class GrowingDemo {

    public static void main(String[] args) throws Exception {
        GrowingAPI api = GrowingAPI.apiInstance();

        for (int i = 0; i < 5000; i++) {
            Map<String, Object> commonAttrs = new HashMap<String, Object>();
            commonAttrs.put("u", "u1");
            commonAttrs.put("s", "s1");
            commonAttrs.put("d", "www.any.com");
            commonAttrs.put("p", "/path");
            commonAttrs.put("q", "src=1");
//            commonAttrs.put("t", "cstm");           // 若无t字段则丢弃该数据
//            commonAttrs.put("tm", 1234567);
            commonAttrs.put("ptm", 7654321);

            Map<String, Object> properties = new HashMap<String, Object>();
            properties.put("params1", "p1");
            properties.put("params2", "p2");

            // sub properties
            Map<String, Object> sub1 = new HashMap<String, Object>();
            sub1.put("sub", "sub1");

            Map<String, Object> sub2 = new HashMap<String, Object>();
            sub2.put("sub", "sub2");

            List<Map<String, Object>> sub = new ArrayList<Map<String, Object>>();
            sub.add(sub1);
            sub.add(sub2);

            properties.put("params3", sub);

            commonAttrs.put("id", "" + i);
            api.trace("search", commonAttrs, properties);
        }

        api.shutdownNow();
    }
}
