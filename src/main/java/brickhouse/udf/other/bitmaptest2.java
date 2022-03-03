package brickhouse.udf.other;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @author
 * @create 2022/01/12
 */
public class bitmaptest2 extends UDF {
    public String evaluate(Integer x){
        return x.toString();

    }
}
