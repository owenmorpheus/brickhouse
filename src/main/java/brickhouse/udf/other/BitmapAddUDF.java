package brickhouse.udf.other;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author zhangtianyu02
 * @create 2022/01/11
 * st et表示数组的开始结束时间
 * bitmap增量添加方式
 * todayexist =1说明今日也活跃 =0为只在之前活跃。
 */
public class BitmapAddUDF extends UDF {
    public  static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    public byte[] evaluate(byte[] bitmap,String st,String et,int todayexist) throws ParseException {

            if ((datediff(et, st) + 1) % 8 == 0) {
                byte[] newbit = new byte[bitmap.length + 1];
                System.arraycopy(bitmap, 0, newbit, 0, bitmap.length);
                if(todayexist==1){
                setNewBit(newbit, 1);
                return newbit;
                }else{
                    return newbit;
                }
            } else {
                if(todayexist==1){
                int yushu = (datediff(et, st) + 1) % 8;
                setNewBit(bitmap, yushu + 1);
                return bitmap;
                }else{
                    return bitmap;
                }
            }


    }

    public static void setNewBit(byte bytes[],int number){
        int index=bytes.length-1;
        int offset=number-1;
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
        }

    }
    public static int datediff(String timea,String timeb) throws ParseException {
        Date parsea = simpleDateFormat.parse(timea);
        Date parseb = simpleDateFormat.parse(timeb);
        int result=(int)((parsea.getTime()-parseb.getTime())/(24*60*60*1000));
        return result;

    }
}
