package rdd.operate;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import util.Environment;

import java.util.Arrays;

public class JoinOperate {
    public static void main(String[] args) {
        JavaSparkContext javaSparkContext = Environment.getJavaSparkContext();
        JavaRDD<Tuple2> rdd1 = javaSparkContext.parallelize(Arrays.asList(
                new Tuple2("hive", 2),
                new Tuple2("spark", 3),
                new Tuple2("hadoop", 4))
        );

        JavaRDD<Tuple2> rdd2 = javaSparkContext.parallelize(Arrays.asList(
                new Tuple2("spark", 3),
                new Tuple2("hadoop", 4),
                new Tuple2("flume", 5))
        );

        JavaPairRDD javaPairRDD1 = rdd1.mapToPair(x -> x);
        JavaPairRDD javaPairRDD2 = rdd2.mapToPair(x -> x);

        System.out.println("-------join------");
        javaPairRDD1.join(javaPairRDD1).collect().forEach(System.out::println);

        System.out.println("-------left join------");
        javaPairRDD1.leftOuterJoin(javaPairRDD2).collect().forEach(System.out::println);

        System.out.println("-------right join------");
        javaPairRDD1.rightOuterJoin(javaPairRDD2).collect().forEach(System.out::println);

        System.out.println("-------full join------");
        javaPairRDD1.fullOuterJoin(javaPairRDD2).collect().forEach(System.out::println);
    }
}

/*
* -------join------
(hive,(2,2))
(spark,(3,3))
(hadoop,(4,4))
-------left join------
(hive,(2,Optional.empty))
(spark,(3,Optional[3]))
(hadoop,(4,Optional[4]))
-------right join------
(spark,(Optional[3],3))
(hadoop,(Optional[4],4))
(flume,(Optional.empty,5))
-------full join------
(hive,(Optional[2],Optional.empty))
(spark,(Optional[3],Optional[3]))
(hadoop,(Optional[4],Optional[4]))
(flume,(Optional.empty,Optional[5]))

*
* */
