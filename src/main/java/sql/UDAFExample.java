package sql;

import org.apache.spark.sql.*;
import util.Environment;
import util.SparkSqlUDAF;

public class UDAFExample {
    public static void main(String[] args) {
        SparkSession sparkSession = Environment.getSparkSession();
        Dataset<Row> json = sparkSession
                .read()
                .json("data/user.txt");
        // 注册udf函数 returnType:StringType 需要导入静态内部类才可使用 scala 没有这么麻烦
        sparkSession.udf().register("avgAge",functions.udaf(new SparkSqlUDAF(), Encoders.LONG()));
        json.createOrReplaceTempView("user");
        Dataset<Row> user = sparkSession.sql("select avgAge(age) as name from user");
        user.show();
        sparkSession.close();
    }
}
