package rdd.operate;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.Arrays;


/*
* Fold 算子 操作演示
* 指定的初始值进行计算 先在分区内初始值计算 最后在分区间进行计算
* */
public class FoldOperate {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("Fold");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6),1);
        Integer foldRdd = rdd.fold(10, (x, y) -> x + y);
        System.out.println(foldRdd);
        jsc.close();
        jsc.stop();
    }
}

/*
 * 65
 * */