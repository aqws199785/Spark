package example;

import entry.HotCategory;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import util.Environment;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/*
* 获取热门商品各个品类的前三
* */

public class HotCategoryCount {
    public static void main(String[] args) {
        JavaSparkContext jsc = Environment.getJavaSparkContext();
        JavaRDD<String> rdd = jsc.textFile("data\\user_visit_action.txt");
        rdd.filter(line->line.split("\t")[5].equals("\\N"))
                .map(line -> {
            String[] array = line.split("\t");
            if (!"-1".equals(array[6])) {
                return new HotCategory(array[6], 1L, 0L, 0L);
            } else if (!"\\N".equals(array[8])) {
                return new HotCategory(array[8], 0L, 1L, 0L);
            } else {
                return new HotCategory(array[10], 0L, 0L, 1L);
            }
        })
                .flatMapToPair(hc -> {
                    ArrayList<Tuple2<String, HotCategory>> arrayList = new ArrayList<>();

                    String[] idArray = hc.getId().split(",");
                    for (String id : idArray) {
                        arrayList.add(new Tuple2<>(id, new HotCategory(id,hc.getClickCount(),hc.getOrderCount(),hc.getPayCount())));
                    }
                    return arrayList.iterator();
                })
                .reduceByKey((hc1, hc2) -> {
                            if (!hc1.getId().equals(hc2.getId())) {
                                System.out.println(hc1.getId() + "\t" + hc2.getId());
                            }
                            return new HotCategory(
                                    hc1.getId(),
                                    hc1.getClickCount() + hc2.getClickCount(),
                                    hc1.getOrderCount() + hc2.getOrderCount(),
                                    hc1.getPayCount() + hc2.getPayCount()
                            );
                        }


                )
                .groupBy(x->1)
                .mapValues(item-> {
                    List<Tuple2<String,HotCategory>> list = IteratorUtils.toList(item.iterator());
                    // 不建议使用这种排序
//                    list.sort(new Comparator<Tuple2<String, HotCategory>>() {
//                        @Override
//                        public int compare(Tuple2<String, HotCategory> o1, Tuple2<String, HotCategory> o2) {
//                            return o1._2.getClickCount().compareTo(o2._2.getClickCount());
//                        }
//                    });

                    // 排序 可以获取 点击,下单,支付 各个类型的前三
                    Stream<Tuple2<String, HotCategory>> sorted = list.stream().sorted(new Comparator<Tuple2<String, HotCategory>>() {
                        @Override
                        public int compare(Tuple2<String, HotCategory> o1, Tuple2<String, HotCategory> o2) {
                            return o1._2.getClickCount().compareTo(o2._2.getClickCount());
                        }
                    }).limit(3);

                    return sorted;
                })
                .flatMap(x->x._2.iterator())
                .collect()
                .forEach(System.out::println);
        jsc.close();
        jsc.stop();
    }
}


