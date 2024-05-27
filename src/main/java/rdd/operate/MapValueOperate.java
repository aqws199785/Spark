package rdd.operate;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import util.Environment;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


/*
* 演示mapValue算子使用
* 注意：该算子只能在groupBy后可以使用
* 统计奇数偶数分别的和
* */
public class MapValueOperate {
    public static void main(String[] args) {
        JavaSparkContext javaSparkContext = Environment.getJavaSparkContext();
        List<Integer> list = Arrays.asList(1, 2, 3, 4);
        JavaRDD<Integer> javaRDD = javaSparkContext.parallelize(list);
        javaRDD.groupBy(num -> num % 2)
                .mapValues(item -> {
                    int sum = 0;
                    Iterator<Integer> iterator = item.iterator();

                    while (iterator.hasNext()) {
                        sum += iterator.next();
                    }
                    return sum;
                })
                .collect()
                .forEach(System.out::println);
        javaSparkContext.close();
    }
}
