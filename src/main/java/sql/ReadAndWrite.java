package sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class ReadAndWrite {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .enableHiveSupport()    // 开启hive的支持
                .master("local[*]")
                .appName("SparkSql")
                .getOrCreate();

        hive(sparkSession);

        sparkSession.close();
    }

    public void readCsv(SparkSession sparkSession) {
        Dataset<Row> csv = sparkSession.read()
                .option("header", "true") //是否用表头作为列名
                .option("seq", ",")  //列分割符
                .csv("/data/user.csv");

        csv.write()
                .option("header","true")
                .mode("append")
                .csv("output");
    }
    /*
    * parquet 是列式存储的一种格式
    * */
    public static void parquet(SparkSession sparkSession){
        Dataset<Row> parquet = sparkSession.read()
                .parquet("path");

        parquet.write()
                .parquet("path");
    }

    /*
    * Spark jdbc的读写方式
    * */
    public static void jdbc(SparkSession sparkSession){
        Properties properties = new Properties();
        properties.setProperty("user","root");
        properties.setProperty("password","");


        Dataset<Row> jdbc = sparkSession.read().jdbc("url", "table", "columnName", 1L, 2L, 3, properties);

        // 写入建议添加模式
        jdbc.write()
                .mode(SaveMode.Append)
                .jdbc("url","table",properties);

    }

    /*
    * Spark hive 的读写方式
    * 需要将hive-site.xml拷贝到resources中 操作hadoop需要hdfs-site.xml core-site.xml yarn-site.xml
    * target 文件夹中药有这些配置 运行项目代码才可读取到hdfs配置
    * 创建 SparkSession 时需要调用enableHiveSupport() 才能开启hive支持
    * */
    public static void hive(SparkSession sparkSession){
        // 必须要设置的配置 window下是用你的当前用户去登录hadoop 当前用户可能没有权限
        System.setProperty("HADOOP_USER_NAME","root");
        // 你可以在sql()方法中编写hive sql语句
        sparkSession.sql("show databases").show();
    }
}
