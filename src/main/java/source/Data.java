package source;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.Arrays;
import java.util.List;

public class Data {
    /*
     * 数据来源于内存
     */
    public static JavaRDD<String> getDataFromMemory(JavaSparkContext jsc) {
        List<String> list = Arrays.asList("zhangsan", "lishi", "wangwu");
        JavaRDD<String> stringJavaRDD = jsc.parallelize(list);
        return stringJavaRDD;
    }

    /*
     * 数据来源于磁盘
     */
    public static JavaRDD<String> getDataFromDisk(JavaSparkContext jsc,String fileSystem) {
        JavaRDD<String> stringJavaRDD;
        switch (fileSystem) {
            case "file":
                stringJavaRDD = jsc.textFile("D:\\project\\Spark\\data\\test.txt");
            case "hdfs":
                stringJavaRDD = jsc.textFile("hdfs://person:8020/temp/test.txt");
            default:
                stringJavaRDD = jsc.textFile("D:\\project\\Spark\\data\\test.txt");
        }
        return stringJavaRDD;
    }

//    public static JavaRDD<String> getDataFromMemory(SparkContext sc){
//        List<String> list = Arrays.asList("zhangsan", "lishi", "wangwu");
//
//        sc.makeRDD(list,2,3)
//        return parallelize;
//    }
}
