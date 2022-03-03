package brickhouse.udf.other;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;
import java.util.List;

/**
 * @author
 * @create 2022/01/12
 */
public class bitmaptest extends UDF {
    public String evaluate(List<Integer> array){
        Integer integer = array.get(1);
        return integer.toString();


    }
}
