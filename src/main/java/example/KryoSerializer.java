package example;

import entry.HotCategory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
/*
* 强烈建议使用这种序列话方式
* 比java的默认序列化方式快10倍
* */
public class KryoSerializer {
    public static void main(String[] args) throws ClassNotFoundException {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[4]");
        conf.setAppName("Kryo");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.registerKryoClasses(new Class[]{Class.forName("entry.HotCategory")});

        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("OFF");
        HotCategory hotCategory = new HotCategory("1", 1L, 2L, 3L);
        JavaRDD<HotCategory> rdd = jsc.parallelize(Arrays.asList(hotCategory));
        rdd.collect()
                .forEach(System.out::println);
        jsc.close();
        jsc.stop();

    }
}
