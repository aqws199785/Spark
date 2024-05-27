package rdd.operate;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import util.Environment;

import java.util.Arrays;
import java.util.List;
/*
* SortBy算子演示
* sortBy(排序的值，是否升序，分区数)
* 该算子是有shuffle过程的
* */

public class SortByOperate {
    public static void main(String[] args) {
        JavaSparkContext javaSparkContext = Environment.getJavaSparkContext();
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 4, 6, 3, 4, 6);
        JavaRDD<Integer> javaRDD = javaSparkContext.parallelize(list);
        javaRDD.sortBy(num->num,true,2)
                .collect()
                .forEach(System.out::println);
        javaSparkContext.close();
    }
}
