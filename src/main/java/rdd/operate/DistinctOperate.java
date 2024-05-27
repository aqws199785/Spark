package rdd.operate;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import util.Environment;
import java.util.Arrays;
import java.util.List;

/*
* Distinct算子演示
* 分布式去重 分组加shuffle
* */
public class DistinctOperate {
    public static void main(String[] args) {
        JavaSparkContext javaSparkContext = Environment.getJavaSparkContext();
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 4, 6, 3, 4, 6);
        JavaRDD<Integer> javaRDD = javaSparkContext.parallelize(list);
        javaRDD.distinct()
                .collect()
                .forEach(System.out::println);
        javaSparkContext.close();
    }
}
