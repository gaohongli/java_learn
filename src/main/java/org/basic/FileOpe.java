package org.basic;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class FileOpe {
    /**
     * 按行读取文件全部内容
     */
    public static void readAllFile(){
        String content="";
        StringBuilder builder = new StringBuilder();
        File file = new File("src/main/resources/text.txt");
        try {
            InputStreamReader streamReader = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8);
            BufferedReader bufferedReader=new BufferedReader(streamReader);
            while ((content=bufferedReader.readLine())!= null){
                builder.append(content);
            }
            System.out.println(builder.toString());
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
    }
}
