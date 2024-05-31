package util;

import entry.Buffer;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

import java.math.BigDecimal;
import java.math.RoundingMode;


public class SparkSqlUDAF extends Aggregator<Long,Buffer, BigDecimal> {

    /*
    * 初始化缓冲区
    * */
    @Override
    public Buffer zero() {
        return new Buffer(0L,0L);
    }

    /*
    * 将数据与缓冲区合并
    * */
    @Override
    public Buffer reduce(Buffer buffer, Long a) {
        buffer.setB1(buffer.getB1()+a);   // 计算值之和
        buffer.setB2(buffer.getB2()+1L);   // 计算数量
        return buffer;
    }


    /*
    * 合并缓冲区
    * */
    @Override
    public Buffer merge(Buffer buffer1, Buffer buffer2) {
        buffer1.setB1(buffer1.getB1()+buffer2.getB1());

        buffer1.setB2(buffer1.getB2()+buffer2.getB2());
        return buffer1;
    }

    @Override
    public BigDecimal finish(Buffer reduction) {
        System.out.println(reduction.toString());
        long result = reduction.getB1() / reduction.getB2();
        BigDecimal bigDecimal = new BigDecimal(result);
        System.out.println(bigDecimal.setScale(2, RoundingMode.HALF_UP));
        return bigDecimal.setScale(2, RoundingMode.HALF_UP);
    }

    @Override
    public Encoder<Buffer> bufferEncoder() {
        return Encoders.bean(Buffer.class);
    }

    @Override
    public Encoder<BigDecimal> outputEncoder() {
        return Encoders.DECIMAL();
    }
}
