package com.assignment3.spark;

import java.util.Arrays;
import java.util.Iterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.SparkFiles;
import java.util.List;
import java.util.ArrayList;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import scala.Tuple2;

import org.apache.spark.sql.Dataset;

public class WordCount {

    // deifning static inner class implementing interface flatMapFunction to process each line of input
    static class Filter implements FlatMapFunction<String, String>
    {
        @Override
        public Iterator<String> call(String s) {
            /*
             * add your code to filter words
             */
            // proccessinf of each line s
            String[] words = s.split("\\s+");
            List<String> filteredWords = new ArrayList<>();
            for (String word : words) {
                // Clean each word and convert it to lowercase
                word = word.replaceAll("[^a-zA-Z]", "").toLowerCase();
                if (!word.isEmpty()) {
                    // adding word to list if it is not empty
                    filteredWords.add(word);
                }
            }
            // returning an iterator over the array of words
            return filteredWords.iterator();
        }

    }

    // starting spark application
    public static void main(String[] args) {
        // task1:
        //String textFilePath = "input/pigs.txt"; // update to HDFS url for task2
        String textFilePath = "hdfs://192.168.188.21:9870/sparkApp/input/pigs.txt"; // update to HDFS url for task2
        SparkConf conf = new SparkConf().setAppName("WordCountWithSpark").setMaster("spark://192.168.188.21:7077"); // task2: update the setMaster with your cluster master URL for executing this code on the cluster
        // create JavaSparkContext object as an entry point to spark
        JavaSparkContext sparkContext =  new JavaSparkContext(conf);
        // create string out of input file (for parallel distribution), read into RDD data structure of strings
        JavaRDD<String> textFile = sparkContext.textFile(textFilePath);

        // in the flatmap call on a RDD, the call method is internally called
        // textFile = RDD of strings / lines
        JavaRDD<String> words = textFile.flatMap(new Filter());
        // result: new RDD object, each line split into words

        /*
         * add your code for key value mapping
         *
         * add your code to perform reduce on the given key value pairs
         *
         * print the word count and save the output in the format, e.g.,(in:15) to an 'output' folder (on HDFS for task 2)
         * try to consolidate your output into single text file if you want to check your output against the given sample output
         */

        //mapping to key (word) and value (1) pairs
        // Convert each word into a key-value pair (word, 1)
        JavaPairRDD<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
        // Reduce the pairs by key (word) to sum up the counts
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(Integer::sum);

        // To consolidate output into one file, coalesce the RDD before saving
        counts.saveAsTextFile("hdfs://192.168.188.21:9870/sparkApp/output_task2_final");
        //counts.coalesce(1).saveAsTextFile("/sparkApp/output2");

        // Collect the word count results and print them to the console
        // goal: achieve non empty log files
        List<Tuple2<String, Integer>> countList = counts.collect();
        for (Tuple2<String, Integer> count : countList) {
            System.out.println(count._1() + ": " + count._2());
        }

        sparkContext.stop();
        sparkContext.close();
    }
}

