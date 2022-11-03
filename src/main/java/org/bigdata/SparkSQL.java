package org.bigdata;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.utils.Common;

import java.util.List;

public class SparkSQL {
    public static void main(String[] args) throws AnalysisException {
        SparkSession sparkSession= SparkSession.builder().config(Common.getSparkConf()).getOrCreate();
        SQLContext sqlContext = new SQLContext(sparkSession);

        //读取hdfs文件，通过Spark SQL查询过滤
        DataFrameReader dataFrameReader = sqlContext.read();
        Dataset<Row> dataset=dataFrameReader.format("json").load(Common.RESOURCE_PATH+"/people.json");
        dataset.show();

        dataset.createTempView("people");
        Dataset<Row> teenagers = sqlContext.sql("SELECT name FROM people WHERE age < 30");
        teenagers.show();
        List<String> teenagerList = teenagers.toJavaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row row){
                return "name:"+row.getString(0);
            }
        }).collect();
        for (String name : teenagerList){
            System.out.println(name);
        }

        teenagers.write().format("json").mode(SaveMode.Overwrite).save(Common.RESOURCE_PATH+"/people_filter");

    }
}
