package sql;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import util.Environment;

public class SelectUserInfo {
    public static void main(String[] args) throws AnalysisException {
        SparkSession sparkSession = Environment.getSparkSession();
        Dataset<Row> json = sparkSession
                .read()
                .json("data/user.txt");
        json.createGlobalTempView("user");
        Dataset<Row> user = sparkSession.sql("select * from global_temp.user");
        user.show();
        sparkSession.close();
    }
}
