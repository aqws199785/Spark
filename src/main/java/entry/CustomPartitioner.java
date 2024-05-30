package entry;

import org.apache.spark.Partitioner;

public class CustomPartitioner extends Partitioner {

    private final int numPartitions;

    public CustomPartitioner(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    /*
     * 设置分区器分几个区
     * */
    @Override
    public int numPartitions() {
        return this.numPartitions;
    }

    /*
     * 分配分区号的规则
     * */
    @Override
    public int getPartition(Object key) {
        return key.toString().length() % 3;
    }


    // 如下两个方法的重写是有必要的
    // 在 rdd.reduceByKey(new CustomPartitioner(num),....)之后在调用reduceByKey(new CustomPartitioner(num),....)
    // 不会产生新的分区 和新的stage划分：不会产生shuffle
    @Override
    public int hashCode() {
        return numPartitions;
    }

    @Override
    public boolean equals(Object obj) {
        // 如果两个实例相同
        if (obj instanceof CustomPartitioner) {
            //将实例强制转换
            CustomPartitioner other = (CustomPartitioner) obj;
            return this.numPartitions == other.numPartitions;
        } else {
            return false;
        }
    }
}
