package org.utils;
import org.apache.spark.SparkConf;

import java.io.File;
public class Common {
    public static final String RESOURCE_PATH = new File(".").getAbsolutePath()+"/src/main/resources";
    public static final String SPARK_REMOTE_SERVER_ADDRESS = "spark://192.168.1.128:7077";
    public static final String HDFSUrlPre = "hdfs://192.168.146.128:9000";
    public static final String HDFSWorkPath = HDFSUrlPre + "/user/root/";
    public static SparkConf getSparkConf(){
        SparkConf conf=new SparkConf();
        conf.setAppName("WordCountJava");
        conf.setMaster("local").set("spark.testing.memory","1147480000"); //本地调试使用，提交到集群请注释掉
        return conf;
    }
}
