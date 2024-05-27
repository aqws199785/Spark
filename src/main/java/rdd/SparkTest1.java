package rdd;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import source.Data;
import util.Environment;

import java.util.List;

public class SparkTest1 {
    public static void main(String[] args) {
        JavaSparkContext jsc = Environment.getJavaSparkContext();
        JavaRDD<String> dataFromMemory = Data.getDataFromMemory(jsc);
        List<String> collect = dataFromMemory.collect();
        collect.forEach(System.out::println);
    }
}
