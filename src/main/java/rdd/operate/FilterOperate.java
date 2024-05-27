package rdd.operate;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import source.Data;
import util.Environment;
/*
* Filter 算子演示
* 过滤长度大于5的数据
* */
public class FilterOperate {
    public static void main(String[] args) {
        JavaSparkContext jsc = Environment.getJavaSparkContext();
        JavaRDD<String> dataFromMemory = Data.getDataFromMemory(jsc);
        JavaRDD<String> map = dataFromMemory.filter(str -> str.length()>5);
        map.collect().forEach(System.out::println);
        // 注意关闭连接
        jsc.close();
    }
}
