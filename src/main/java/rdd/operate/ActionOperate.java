package rdd.operate;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import util.Environment;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class ActionOperate implements Serializable {
    public static void main(String[] args) {
        JavaSparkContext jsc = Environment.getJavaSparkContext();
        List<Integer> list = Arrays.asList(1, 3, 2, 4, 4);
        JavaRDD<Integer> rdd = jsc.parallelize(list);
        System.out.println(rdd.first());
        System.out.println(rdd.distinct());
        rdd.takeOrdered(3).forEach(System.out::println);
    }
}
