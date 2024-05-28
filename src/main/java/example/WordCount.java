package example;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.ReduceFunction;
import scala.Tuple2;
import scala.Tuple3;
import util.Environment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public class WordCount {
    public static void main(String[] args) {
        JavaSparkContext javaSparkContext = Environment.getJavaSparkContext();
        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile("D:\\project\\Spark\\data\\word.txt");

        WordCount.mapValuesExample(stringJavaRDD);
        WordCount.groupByKeyExample(stringJavaRDD);
        WordCount.reduceByKeyExample(stringJavaRDD);
        WordCount.sortByKeyExample(stringJavaRDD);
        javaSparkContext.close();
        javaSparkContext.stop();
    }

    /*
     * 演示使用mapValues进行WordCount
     * 按照key将数据放到同一组
     * 缺点：使用该方式进行WordCount 各数据依旧在各个分区 无法进行按照值来排序 只能进行按照key排序
     * */
    public static void mapValuesExample(JavaRDD<String> stringJavaRDD) {
        System.out.println("---------MapValues------------");
        stringJavaRDD
                .flatMap(
                        str -> Arrays.asList(str.split("\\s")).iterator()
                )
                .groupBy(word -> word)
                .mapValues(iter -> {
                            int num = 0;
                            for (String s : iter) {
                                num++;
                            }
                            return num;
                        }
                )
                .collect()
                .forEach(System.out::println);
    }
    /*
     * groupByKey 演示使用案例
     * 按照key将value放到同一组
     *
     * */

    public static void groupByKeyExample(JavaRDD<String> stringJavaRDD) {
        System.out.println("-----------GroupByKey----------");
        stringJavaRDD.flatMapToPair(
                str -> {
                    ArrayList<Tuple2<String, Integer>> arrayList = new ArrayList<Tuple2<String, Integer>>();
                    String[] wordArray = str.split("\\s");
                    for (String word : wordArray) {
                        arrayList.add(new Tuple2(word, 1));
                    }
                    return arrayList.iterator();
                }
        )
                .groupByKey()
                .mapValues(iter -> {
                    int num = 0;
                    for (Integer v : iter) {
                        num += v;
                    }
                    return num;
                })
                .collect()
                .forEach(System.out::println);
    }


    public static void reduceByKeyExample(JavaRDD<String> stringJavaRDD) {
        System.out.println("----------ReduceByKey----------");
        stringJavaRDD.flatMapToPair(
                str -> {
                    ArrayList<Tuple2<String, Integer>> arrayList = new ArrayList<Tuple2<String, Integer>>();
                    String[] wordArray = str.split("\\s");
                    for (String word : wordArray) {
                        arrayList.add(new Tuple2(word, 1));
                    }
                    return arrayList.iterator();
                })
                .reduceByKey((x, y) -> x + y)
                .collect()
                .forEach(System.out::println);
    }

    /*
     * sortByKey 演示使用案例
     * 功能；按照key对数据进行排序 字符串为字典序；数值为大小
     *
     * */
    public static void sortByKeyExample(JavaRDD<String> stringJavaRDD) {
        System.out.println("-----------SortByKey----------");
        stringJavaRDD.flatMapToPair(
                str -> {
                    ArrayList<Tuple2<String, Integer>> arrayList = new ArrayList<Tuple2<String, Integer>>();
                    String[] wordArray = str.split("\\s");
                    for (String word : wordArray) {
                        arrayList.add(new Tuple2(word, 1));
                    }
                    return arrayList.iterator();
                }
        )
                .groupByKey()
                .sortByKey()
                .collect()
                .forEach(System.out::println);
    }
}
