package brickhouse.udf.other;


import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhangtianyu02
 * @create 2022/01/12
 */
public class bitmapreturn extends UDF {
    public List<Integer> evalutae(String a){
        Integer x = Integer.valueOf(a);
        List<Integer> list =new ArrayList<Integer>();
        list.add(x);
        list.add(x);
        return list;
    }

}
