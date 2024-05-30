package sql;

import org.apache.spark.sql.SparkSession;

public class ReadAndWrite {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .appName("SparkSql")
                .getOrCreate();

        sparkSession.close();
    }

    public void readCsv(SparkSession sparkSession) {
        sparkSession.read()
                .option("header", "true") //是否用表头作为列名
                .option("seq", ",")  //列分割符
                .csv("/data/user.csv");
    }
}
