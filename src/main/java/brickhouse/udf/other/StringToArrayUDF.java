package brickhouse.udf.other;

import com.alibaba.fastjson.JSONArray;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.List;

/**
 * @author
 * @create 2022/02/25
 */
public class StringToArrayUDF extends UDF {
    public String evaluate(String js){
        JSONArray objects = JSONArray.parseArray(js);
        StringBuffer sb =new StringBuffer("");

        for(Object x:objects){
            sb.append(x.toString());
            sb.append("|");
        }
        return sb.toString().substring(0,sb.length()-1);

    }
}
