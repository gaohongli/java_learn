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
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Iterator;

public class WordCount {
    {
        System.setProperty("HADOOP_USER_NAME", "root");
    }
    public static final String HDFSUrlPre = "hdfs://192.168.146.128:9000/user/root/";
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
        String content;
        StringBuilder builder = new StringBuilder();
        File file = new File("src/main/resources/text.txt");
        String hdfsFilePath = WordCount.HDFSUrlPre+"text.txt";
        try {
            //读取文件
            InputStreamReader streamReader = new InputStreamReader(Files.newInputStream(file.toPath()), StandardCharsets.UTF_8);
            BufferedReader bufferedReader=new BufferedReader(streamReader);
            while ((content=bufferedReader.readLine())!= null){
                builder.append(content);
            }

            //写入hdfs
            Configuration conf = new Configuration();
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
        SparkConf conf=new SparkConf();
        conf.setAppName("WordCountJava")
                .setMaster("local").set("spark.testing.memory","1147480000");

        try(JavaSparkContext sc=new JavaSparkContext(conf);){
            String path= WordCount.HDFSUrlPre+"/text.txt";

            JavaRDD<String> linesRDD=sc.textFile(path);
            //flatMap和mapToPair都是对RDD中的元素调用指定函数，区别在于参数和返回值
            JavaRDD<String> wordsRDD = linesRDD.flatMap((FlatMapFunction<String, String>) line -> Arrays.asList(line.split(" ")).iterator());
            JavaPairRDD<String,Integer> pairRDD = wordsRDD.mapToPair((PairFunction<String, String, Integer>) word -> new Tuple2<String,Integer>(word,1));
            //reduceByKey合并key
            JavaPairRDD<String,Integer> wordCountRDD = pairRDD.reduceByKey((Function2<Integer, Integer, Integer>) (integer, integer2) -> integer+integer2);

            String outputPath =  WordCount.HDFSUrlPre+"wordCount";

            Configuration outputConf = new Configuration();
            FileSystem fileSystem = FileSystem.get(outputConf);
            Path writePath = new Path(outputPath);
            fileSystem.delete(writePath,true);
            wordCountRDD.saveAsHadoopFile(outputPath, Text.class,Text.class, TextOutputFormat.class);
            sc.stop();
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
    }
}