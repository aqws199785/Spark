package example;

import entry.CustomPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;

public class Partitioner {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local[4]")
                .setAppName("Cache");

        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setCheckpointDir("cp");
        jsc.setLogLevel("ERROR");
        JavaRDD<Integer> rdd = jsc.parallelize(Arrays.asList(1, 2, 2, 1));
        rdd.mapPartitionsToPair(iter->{
            ArrayList<Tuple2<Integer,Integer>> list = new ArrayList();
            while (iter.hasNext()){
                list.add(new Tuple2(iter.next(),1));
            }
            return list.iterator();
        })
                .reduceByKey(new CustomPartitioner(3),(x,y)->x+y)
                .collect()
                .forEach(System.out::println);

        jsc.close();
        jsc.stop();
    }
}
