package com.alklid.spark.service;

import com.alklid.spark.util.SparkContextUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;

@Service
public class RDDService {

    @Autowired
    private SparkContextUtil sparkContextUtil;

    public String createRDD() {

        String responseData = "";

        // Step1: SparkContext create
        JavaSparkContext sc = sparkContextUtil.getSparkContext("createRDD");

        try {
            JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e"));
            System.out.println("partition count : " + rdd1.partitions().size());

            JavaRDD<String> rddUpper = rdd1.map((String x) -> x.toUpperCase());
            responseData = rddUpper.collect().toString();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            sc.stop();
        }

        return responseData;
    }

}
