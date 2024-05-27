package rdd.operate;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import source.Data;
import util.Environment;

/*
* 演示使用map算子
* 获取每行字符串的长度
* rdd的执行顺序为数据跑完全部算子 再跑下一个数据
* 而不是全部数据跑完一个算子 再跑下一个算子
* */
public class MapOperate {
    public static void main(String[] args) {
        JavaSparkContext jsc = Environment.getJavaSparkContext();
        JavaRDD<String> dataFromMemory = Data.getDataFromMemory(jsc);
        JavaRDD<Integer> map = dataFromMemory.map(str -> str.length());
        map.collect().forEach(System.out::println);
        // 注意关闭连接
        jsc.close();
    }
}
