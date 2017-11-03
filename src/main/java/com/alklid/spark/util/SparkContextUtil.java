package com.alklid.spark.util;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
public class SparkContextUtil {

    @Autowired
    private Environment env;

    public JavaSparkContext getSparkContext(String appName) {
        SparkConf conf = new SparkConf()
                                .setAppName(appName)
                                .setMaster("local[*]")
                                .set("spark.local.ip", "192.168.10.208")
                                .set("spark.driver.host", "192.168.10.208");
        //conf.set("spark.local.ip", env.getProperty("spark.local.ip"));
        //conf.set("spark.driver.host", env.getProperty("spark.driver.host"));
        return new JavaSparkContext(conf);
    }

    public SparkSession getSparkSession(String appName) {
        SparkSession sparkSession = SparkSession.builder().appName(appName)
                .config("spark.sql.broadcastTimeout", env.getProperty("spark.sql.broadcastTimeout"))
                .config("spark.driver.memory", env.getProperty("spark.driver.memory"))
                .config("spark.executor.memory", env.getProperty("spark.executor.memory"))
                .config("spark.ui.enabled", "false")
                .config("spark.eventLong.enabled", "false")
                .master(env.getProperty("spark.master")).getOrCreate();
        return sparkSession;
    }
}
