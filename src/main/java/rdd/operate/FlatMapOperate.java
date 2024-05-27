package rdd.operate;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import source.Data;
import util.Environment;

import java.util.Arrays;
import java.util.List;

/*
 * 演示FlatMap算子使用方式
 *  将数据扁平化 将集合中的数据拆分为独立的个体
 * */
public class FlatMapOperate {
    public static void main(String[] args) {
        JavaSparkContext jsc = Environment.getJavaSparkContext();
        List<List<Integer>> lists = Arrays.asList(
                Arrays.asList(1, 2),
                Arrays.asList(3, 4)
        );
        JavaRDD<List<Integer>> listJavaRDD = jsc.parallelize(lists);
        // flatMap 需要返回一个迭代器
        JavaRDD<Integer> map = listJavaRDD.flatMap(list->list.iterator());
        map.collect().forEach(System.out::println);
        // 注意关闭连接
        jsc.close();
    }
}
