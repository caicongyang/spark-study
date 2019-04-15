package com.caicongyang.spark.study.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author caicongyang
 * @version id: GsonUtils, v 0.1 17/12/21 上午11:30 caicongyang1 Exp $$
 */
public class GsonUtils {

    public static final Logger LOGGER = LoggerFactory.getLogger(GsonUtils.class);

    private static Gson gson = new GsonBuilder().serializeNulls().disableHtmlEscaping().create();

    /**
     * 对象转json
     */
    public static String toJson(Object object) {
        return gson.toJson(object);
    }

    /**
     * json转对象
     */
    public static <T> T fromJson(String json, Class<T> klass) {
        T object = null;
        return gson.fromJson(json, klass);
    }

}
