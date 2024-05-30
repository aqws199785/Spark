package util;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/*
 * 执行环境工具类
 * */

public class Environment {
    public static JavaSparkContext getJavaSparkContext() {
        SparkConf conf = new SparkConf();
        // local[num]为对应的并行度 设置分区数为num
        conf.setMaster("local[4]");
        conf.setAppName("spark");
        // 分区数参数
        // conf.set("spark.default.parallelism" ,"4");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        javaSparkContext.setLogLevel("OFF");
        return javaSparkContext;
    }

    public static SparkContext getSparkContext() {
        SparkConf conf = new SparkConf();
        // local[num]为对应的并行度 设置分区数为num
        conf.setMaster("local[4]");
        conf.setAppName("spark");
        SparkContext sparkContext = new SparkContext(conf);
        sparkContext.setLogLevel("OFF");
        return sparkContext;
    }

    public static SparkSession getSparkSession(){
        SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .appName("SparkSql")
                .getOrCreate();
        return sparkSession;
    }
}