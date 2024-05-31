package util;

import org.apache.spark.sql.api.java.UDF1;
/*
* 将数据加上前缀
* */
public class SparkSqlUDF implements UDF1 {
    @Override
    public Object call(Object o) {
        return String.format("%s:%s","value",o.toString());
    }
}
