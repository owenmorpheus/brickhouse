package brickhouse.udf.csvreader;

/**
 * @author
 * @create 2022/02/11
 */
import brickhouse.udf.httputil.HttpsUDF;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;



/*
 *包名:com.kdzwy.cases
 *作者:Adien_cui
 *时间:2017-9-25  下午4:36:29
 *描述:读取csv文件
 **/
public class csvteset {
    public static void readCsvFile(String filePath){
        try {
            ArrayList<String[]> csvList = new ArrayList<String[]>();
            CsvReader reader = new CsvReader(filePath,',',Charset.forName("UTF-8"));
//          reader.readHeaders(); //跳过表头,不跳可以注释掉

            while(reader.readRecord()){
                csvList.add(reader.getValues()); //按行读取，并把每一行的数据添加到list集合
            }
            reader.close();
            System.out.println("读取的行数："+csvList.size());

            HttpsUDF httpsUDF = new HttpsUDF();

            for(int row=0;row<csvList.size();row++){

                //打印每一行的数据
                System.out.print(csvList.get(row)[0]+",");
                System.out.print(csvList.get(row)[1]+",");
                System.out.print(csvList.get(row)[2]+",");
                String str0=csvList.get(row)[0];
                String str00=str0.substring(1,str0.length()-1);
                String str1=csvList.get(row)[1];

                String evaluate = httpsUDF.evaluate(str00, str1);
                System.out.println(evaluate);


            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String filePath = "/Users/admin/Downloads/111.csv";
        readCsvFile(filePath);
    }
}

