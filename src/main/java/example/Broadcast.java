package example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Broadcast {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local[4]")
                .setAppName("Cache");

        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("ERROR");

        JavaRDD<String> rdd = jsc.parallelize(Arrays.asList("hive", "spark", "flink", "spark", "hive"));
        List<String> list = Arrays.asList("hive", "spark");
        jsc.broadcast(list);

        rdd.filter(str->list.contains(str))
                .collect()
                .forEach(System.out::println);
        jsc.close();
        jsc.stop();
    }
}
