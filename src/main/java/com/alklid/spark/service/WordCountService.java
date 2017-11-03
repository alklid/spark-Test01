package com.alklid.spark.service;

import com.alklid.spark.util.SparkContextUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

@Service
public class WordCountService {

    @Autowired
    private SparkContextUtil sparkContextUtil;

    public String wordCountPost(HashMap<String, Object> body) {

        /*
            body
            - inputFile
            - outputDirPath
        */
        String input = body.get("inputFile").toString();
        String output = body.get("outputDirPath").toString();
        long wordCount = 0;

        // Step1: SparkContext create
        JavaSparkContext sc = sparkContextUtil.getSparkContext("WordCount");

        try {
            // Step2: rdd create from input
            JavaRDD<String> inputRDD = getInputRDD(sc, input);

            // Step3: process
            //JavaPairRDD<String, Integer> resultRDD = process(inputRDD);
            JavaPairRDD<String, Integer> resultRDD = processWithLambda(inputRDD);
            wordCount = resultRDD.count();

            // Step4: result save
            handleResult(resultRDD, output);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Step5: SparkContext stop
            sc.stop();
        }

        return String.valueOf(wordCount);
    }

    public static JavaRDD<String> getInputRDD(JavaSparkContext sc, String input) {
        return sc.textFile(input);
    }

    public static JavaPairRDD<String, Integer> process(JavaRDD<String> inputRDD) {
        JavaRDD<String> words = inputRDD.flatMap(new FlatMapFunction<String, String>() {
                                    @Override
                                    public Iterator<String> call(String s) throws Exception {
                                        return Arrays.asList(s.split(" ")).iterator();
                                    }
        });

        JavaPairRDD<String, Integer> wcPair = words.mapToPair(new PairFunction<String, String, Integer>() {
                                    @Override
                                    public Tuple2<String, Integer> call(String s) throws Exception {
                                        return new Tuple2<>(s, 1);
                                    }
        });

        JavaPairRDD<String, Integer> result = wcPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
                                    @Override
                                    public Integer call(Integer v1, Integer v2) throws Exception {
                                        return v1 + v2;
                                    }
        });

        return result;
    }

    public static JavaPairRDD<String, Integer> processWithLambda(JavaRDD<String> inputRDD) {
        JavaRDD<String> words = inputRDD.flatMap((String s) -> Arrays.asList(s.split(" ")).iterator());
        JavaPairRDD<String, Integer> wcPair = words.mapToPair((String w) -> new Tuple2(w, 1));
        JavaPairRDD<String, Integer> result = wcPair.reduceByKey((Integer c1, Integer c2) -> c1 + c2);
        return result;
    }

    public static void handleResult(JavaPairRDD<String, Integer> resultRDD, String output) {
        resultRDD.saveAsTextFile(output);
    }
}
