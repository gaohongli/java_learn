package org.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;

public class WordCount {

    public static void main(String[] args){
        WordCount.oprateHDFS();

        //单词数统计
        WordCount.wordCount();

//        SparkConf sparkConf=new SparkConf().setAppName("WordCountJava").setMaster("local").set("spark.testing.memory","1147480000");
//        SparkSession spark=SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();
//        spark.sql("use test");
//        spark.sql("select * from word_count").show(false);
//        spark.stop();
    }
    private static void oprateHDFS(){
        String content="";
        StringBuilder builder = new StringBuilder();
        File file = new File("src/main/resources/text.txt");
        String hdfsFilePath = "hdfs://192.168.146.128:9000/user/root/text.txt";
        try {
            //读取文件
            InputStreamReader streamReader = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8);
            BufferedReader bufferedReader=new BufferedReader(streamReader);
            while ((content=bufferedReader.readLine())!= null){
                builder.append(content);
            }

            //写入hdfs
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS","hdfs://192.168.146.128:9000");
            conf.set("HADOOP_USER_NAME", "root");

            FileSystem fileSystem = FileSystem.get(conf);
            Path writePath = new Path(hdfsFilePath);
            FSDataOutputStream outputStream = fileSystem.create(writePath);
            outputStream.writeUTF(builder.toString());
            outputStream.close();
            System.out.println("写入hdfs成功");
            //
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
    }
    private static void wordCount(){
        System.setProperty("HADOOP_USER_NAME", "root");
        SparkConf conf=new SparkConf();
        conf.setAppName("WordCountJava")
                .setMaster("local").set("spark.testing.memory","1147480000");
        JavaSparkContext sc=new JavaSparkContext(conf);
        String path="hdfs://192.168.146.128:9000/user/root/text.txt";
        conf.set("fs.defaultFS","hdfs://192.168.146.128:9000");

        JavaRDD<String> linesRDD=sc.textFile(path);
        JavaRDD<String> wordsRDD = linesRDD.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String line) throws Exception{
                return Arrays.asList(line.split(" ")).iterator();
            }
        });
        JavaPairRDD<String,Integer> pairRDD = wordsRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return  new Tuple2<String,Integer>(word,1);
            }
        });
        JavaPairRDD<String,Integer> wordCountRDD = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });
        System.out.println(wordCountRDD.count());
        wordCountRDD.saveAsHadoopFile("hdfs://192.168.146.128:9000/user/root/wordCount", Text.class,Text.class, TextOutputFormat.class);
        sc.stop();
    }
}
