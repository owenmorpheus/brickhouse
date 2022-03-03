package brickhouse.udf.other;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.util.Date;
import java.util.Set;

/**
 * @author zhangtianyu02
 * @create 2022/01/10
 * to_json(collect)得到的数组转化为bitmap空值填补
 * js 数组  st开始时间 et结束时间
 * 一个bit表示一天 只 记录 0 1是否登录
 */
public class BitMapUDF extends UDF {
    public  static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    public String evaluate(String js,String st,String et) throws ParseException {

        byte bitmap[] = new byte[datediff(et,st)/8+1];
        JSONObject object = JSONObject.parseObject(js);
        Set<String> strings = object.keySet();
        for(String strtime:strings){
            if(datediff(et,strtime)>=0&datediff(st,strtime)<=0){
                int number = datediff(strtime,st)+1; //第几个bit 从1开始
                setBit(bitmap,number);
            }
        }
        BytesWritable bw = new BytesWritable(bitmap);
        return "w";

    }


    public static int datediff(String timea,String timeb) throws ParseException {
        Date parsea = simpleDateFormat.parse(timea);
        Date parseb = simpleDateFormat.parse(timeb);
        int result=(int)((parsea.getTime()-parseb.getTime())/(24*60*60*1000));
        return result;

    }

    /**
     *
     * @param bytes bitmap数组
     * @param number 第几个bit至为1 当前时间减去起始时间+1
     */
    public static void setBit(byte bytes[],int number){
        int index;
        int offset;

        if (0 == number % 8) {
            index = number / 8 - 1;
            offset = 7;
        } else {
            index = number / 8;
            offset = number % 8 - 1;
        }
        switch (offset) {
            case 0:
                bytes[index] = (byte) (bytes[index] | 0x01);
                break;
            case 1:
                bytes[index] = (byte) (bytes[index] | 0x02);
                break;
            case 2:
                bytes[index] = (byte) (bytes[index] | 0x04);
                break;
            case 3:
                bytes[index] = (byte) (bytes[index] | 0x08);
                break;
            case 4:
                bytes[index] = (byte) (bytes[index] | 0x10);
                break;
            case 5:
                bytes[index] = (byte) (bytes[index] | 0x20);
                break;
            case 6:
                bytes[index] = (byte) (bytes[index] | 0x40);
                break;
            case 7:
                bytes[index] = (byte) (bytes[index] | 0x80);
                break;
            case 8:
                bytes[index] = (byte) (bytes[index] | 0x100);
                break;
            case 9:
                bytes[index] = (byte) (bytes[index] | 0x200);
                break;
            case 10:
                bytes[index] = (byte) (bytes[index] | 0x400);
                break;
            case 11:
                bytes[index] = (byte) (bytes[index] | 0x800);
                break;
            case 12:
                bytes[index] = (byte) (bytes[index] | 0x1000);
                break;
            case 13:
                bytes[index] = (byte) (bytes[index] | 0x2000);
                break;
            case 14:
                bytes[index] = (byte) (bytes[index] | 0x4000);
                break;
            case 15:
                bytes[index] = (byte) (bytes[index] | 0x8000);
                break;
            case 16:
                bytes[index] = (byte) (bytes[index] | 0x10000);
                break;
            case 17:
                bytes[index] = (byte) (bytes[index] | 0x20000);
                break;
            case 18:
                bytes[index] = (byte) (bytes[index] | 0x40000);
                break;
            case 19:
                bytes[index] = (byte) (bytes[index] | 0x80000);
                break;
            case 20:
                bytes[index] = (byte) (bytes[index] | 0x100000);
                break;
            case 21:
                bytes[index] = (byte) (bytes[index] | 0x200000);
                break;
            case 22:
                bytes[index] = (byte) (bytes[index] | 0x400000);
                break;
            case 23:
                bytes[index] = (byte) (bytes[index] | 0x800000);
                break;
            case 24:
                bytes[index] = (byte) (bytes[index] | 0x1000000);
                break;
            case 25:
                bytes[index] = (byte) (bytes[index] | 0x2000000);
                break;
            case 26:
                bytes[index] = (byte) (bytes[index] | 0x4000000);
                break;
            case 27:
                bytes[index] = (byte) (bytes[index] | 0x8000000);
                break;
            case 28:
                bytes[index] = (byte) (bytes[index] | 0x10000000);
                break;
            case 29:
                bytes[index] = (byte) (bytes[index] | 0x20000000);
                break;
            case 30:
                bytes[index] = (byte) (bytes[index] | 0x40000000);
                break;
            case 31:
                bytes[index] = (byte) (bytes[index] | 0x80000000);
                break;


        }


        }
}
