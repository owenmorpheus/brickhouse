package brickhouse.udf.other;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @author zhangtianyu02
 * @create 2022/01/10
 */
public class weiyunsuanYU extends UDF {


    public int evaluate(int flag,int config){

        try {
            int a = flag & config;
            return a;
        }
        catch (Exception e){

        }
        return -1;
    }

}
