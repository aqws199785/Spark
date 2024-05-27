package rdd.operate;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import util.Environment;

import java.util.Arrays;
import java.util.List;

public class GroupByOperate {
    public static void main(String[] args) {
        JavaSparkContext jsc = Environment.getJavaSparkContext();
        List<Integer> list = Arrays.asList(1, 2, 3, 4);
        JavaRDD<Integer> integerJavaRDD = jsc.parallelize(list);
        JavaPairRDD<Integer, Iterable<Integer>> integerIterableJavaPairRDD = integerJavaRDD.groupBy(num -> num % 2);
        integerIterableJavaPairRDD.collect().forEach(System.out::println);
        jsc.close();
        jsc.stop();
    }
}
/**
 * 输出
*(0,[2, 4])
* (1,[1, 3])
* */