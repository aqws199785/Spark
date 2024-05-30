package example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;
/*
* 演示了cache的使用方式
* 将数据缓存起来，如果有多个计算使用该rdd 血缘关系将会从缓存的rdd开始
* 不使用缓存 血缘关系会从头开始，导致重复计算
* */
public class RddCache {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local[4]")
                .setAppName("Cache");

        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setCheckpointDir("cp");
        jsc.setLogLevel("ERROR");
        JavaRDD<Integer> rdd = jsc.parallelize(Arrays.asList(1, 2, 2, 1));
        JavaPairRDD<Integer, Iterable<Integer>> groupRdd = rdd.map(x ->
                {
                    System.out.println("-----------------------");
                    return x;
                }
        ).groupBy(x -> x);
        // cache 就是代傲用persist(StorageLevel.MEMORY_ONLY())
        groupRdd.cache();
        // 持久化到啊哪里 内存 内存和磁盘 磁盘
        // 存储等级数字代表副本 ser代表序列化
        groupRdd.persist(StorageLevel.MEMORY_AND_DISK());

        // 检查点会在执行一遍rdd的血缘 建议搭配 cache和persist一起使用
        groupRdd.checkpoint();

        groupRdd.collect().forEach(System.out::println);
        jsc.close();
        jsc.stop();
    }
}
