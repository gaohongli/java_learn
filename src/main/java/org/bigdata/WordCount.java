package org.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {
    {
        System.setProperty("HADOOP_USER_NAME", "meepo_test1");
//        System.setProperty("HADOOP_USER_NAME", "root");
    }

//    public static final String HDFSUrlPre = "hdfs://ns1";
    public static final String HDFSUrlPre = "hdfs://192.168.146.128:9000";
//    public static final String HDFSWorkPath = HDFSUrlPre + "/user/meepo_test1/private/test_input/";
    public static final String HDFSWorkPath = HDFSUrlPre + "/user/root/";
    public static final String articlePath = HDFSWorkPath + "article.txt";
    private FileSystem fileSystem;
    public WordCount(){
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS",HDFSUrlPre);
        try {
            fileSystem = FileSystem.get(conf);
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
    }
    public static void main(String[] args){
        WordCount wordCount = new WordCount();
        wordCount.oprateHDFS();

        //单词数统计
        wordCount.wordCount();

//        SparkConf sparkConf=new SparkConf().setAppName("WordCountJava").setMaster("local").set("spark.testing.memory","1147480000");
//        SparkSession spark=SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();
//        spark.sql("use test");
//        spark.sql("select * from word_count").show(false);
//        spark.stop();
    }
    private void oprateHDFS(){
        try {
            //写入hdfs
            Path writePath = new Path(articlePath);
            if(fileSystem.exists(writePath)){
                fileSystem.delete(writePath,false);
            }
            FSDataOutputStream outputStream = fileSystem.create(writePath);
            String writeStr = "hello word hadoop hadoop word hadoop\nEnglish is most A community-based space to find and contribute answers to technical challenges, and one of the most popular websites in the world";
            outputStream.writeUTF(writeStr);
            outputStream.close();
            System.out.println("写入hdfs成功");
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
    }
    private void wordCount(){
        SparkConf conf=new SparkConf();
        conf.setAppName("WordCountJava");
//        conf.setMaster("local").set("spark.testing.memory","1147480000");
        try(JavaSparkContext sc=new JavaSparkContext(conf);){
            JavaRDD<String> linesRDD=sc.textFile(articlePath);
            //flatMap和mapToPair都是对RDD中的元素调用指定函数，区别在于参数和返回值
            JavaRDD<String> wordsRDD = linesRDD.flatMap((FlatMapFunction<String, String>) line -> Arrays.asList(line.split(" ")).iterator());
            JavaPairRDD<String,Integer> pairRDD = wordsRDD.mapToPair((PairFunction<String, String, Integer>) word -> new Tuple2<String,Integer>(word,1));
            //reduceByKey合并key
            JavaPairRDD<String,Integer> wordCountRDD = pairRDD.reduceByKey((Function2<Integer, Integer, Integer>) (integer, integer2) -> integer+integer2);
            String outputPath =  WordCount.HDFSWorkPath+"/wordCount";
            Path writePath = new Path(outputPath);
            if(fileSystem.exists(writePath)){
                fileSystem.delete(writePath,true);
            }
            wordCountRDD.saveAsHadoopFile(outputPath, Text.class,Text.class, TextOutputFormat.class);
            sc.stop();
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
    }
}