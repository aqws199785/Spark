package sql;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import util.Environment;
import util.SparkSqlUDF;

import static org.apache.spark.sql.types.DataTypes.StringType;

public class UDFExample {
    public static void main(String[] args) throws AnalysisException {
        SparkSession sparkSession = Environment.getSparkSession();
        Dataset<Row> json = sparkSession
                .read()
                .json("data/user.txt");
        // 注册udf函数 returnType:StringType 需要导入静态内部类才可使用 scala 没有这么麻烦
        sparkSession.udf().register("prefix",new SparkSqlUDF(),StringType);
        json.createOrReplaceTempView("user");
        Dataset<Row> user = sparkSession.sql("select prefix(name) as name from user");
        user.show(true);
        sparkSession.close();
    }
}
