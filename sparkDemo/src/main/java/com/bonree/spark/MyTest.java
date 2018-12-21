package com.bonree.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.*;

/**
 * 主入口
 */
public class MyTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("application")
                .setMaster("local[1]");//线程数2集群修改为"spark://mini1:7077"
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Map<String, Integer>> maps = new ArrayList<>();
        HashMap<String, Integer> objectObjectHashMap1 = new HashMap<>();
        objectObjectHashMap1.put("a", 1);
        HashMap<String, Integer> objectObjectHashMap2 = new HashMap<>();
        objectObjectHashMap2.put("b", 1);
        HashMap<String, Integer> objectObjectHashMap3 = new HashMap<>();
        objectObjectHashMap3.put("c", 1);
        HashMap<String, Integer> objectObjectHashMap4 = new HashMap<>();
        objectObjectHashMap4.put("d", 1);
        maps.add(objectObjectHashMap1);
        maps.add(objectObjectHashMap2);
        maps.add(objectObjectHashMap2);
        maps.add(objectObjectHashMap2);
        maps.add(objectObjectHashMap3);
        maps.add(objectObjectHashMap3);
        maps.add(objectObjectHashMap4);
        maps.add(objectObjectHashMap4);

        JavaRDD<Map<String, Integer>> mapJavaRDD = sc.parallelize(maps, 8);//读取内存数据,设置8个分区,有shuf过程
        JavaPairRDD<String, Integer> javaPairRDD = mapJavaRDD.mapToPair(new PairFunction<Map<String, Integer>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Map<String, Integer> stringIntegerMap) throws Exception {
                Tuple2<String, Integer> tuple2 = null;
                for (Map.Entry<String, Integer> entry : stringIntegerMap.entrySet()) {
                    tuple2 = new Tuple2<>(entry.getKey(), entry.getValue());
                    break;
                }
                return tuple2;
            }
        });

        JavaPairRDD<String, Integer> javaPairRDD1 = javaPairRDD.aggregateByKey(0, new Function2<Integer, Integer, Integer>() {//0表示聚合的初始值
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {//分区内聚合key
                return integer + integer2;
            }
        }, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {//分区之间聚合key
                return integer + integer2;
            }
        });

        JavaRDD<String> javaRDD = javaPairRDD1.flatMap(new FlatMapFunction<Tuple2<String, Integer>, String>() {//二元变一元
            @Override
            public Iterator<String> call(Tuple2<String, Integer> tuple2) throws Exception {
                List<String> list = new ArrayList();
                for (int i = 0; i < tuple2._2; i++) {
                    list.add(tuple2._1);
                }
                return list.iterator();
            }
        });

        JavaRDD<String> distinct = javaRDD.distinct();//去重
        JavaPairRDD<String, Integer> javaPairRDD2 = distinct.mapToPair(new PairFunction<String, String, Integer>() {//一元变二元
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairRDD<String, Tuple2<Integer, Integer>> join = javaPairRDD2.join(javaPairRDD2);
        JavaPairRDD<String, Integer> javaPairRDD3 = join.mapToPair(new PairFunction<Tuple2<String, Tuple2<Integer, Integer>>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Tuple2<Integer, Integer>> stringTuple2Tuple2) throws Exception {
                Tuple2<Integer, Integer> integerIntegerTuple2 = stringTuple2Tuple2._2;
                return new Tuple2<>(stringTuple2Tuple2._1, integerIntegerTuple2._1 + integerIntegerTuple2._2);
            }
        });

        List<Tuple2<String, Integer>> collect = javaPairRDD3.collect();//算子操作
        for (Tuple2<String, Integer> tuple2 : collect) {
            System.out.println(tuple2._1 + "==" + tuple2._2);
        }

        sc.close();
    }
}
