package brickhouse.udf.other;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author zhangtianyu02
 * @create 2022/01/12
 */
public class BitmapPreNotExistsAddUDF extends UDF {
    public  static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

    public byte[] evaluate(String initime,String today) throws ParseException {
        byte bitmap[] = new byte[datediff(today,initime)/8+1];
        int newIndex=bitmap.length-1;
        int newOffset=(datediff(today,initime)+1)%8-1;
        setBit(bitmap,newIndex,newOffset);
        return bitmap;

    }
    public static int datediff(String timea,String timeb) throws ParseException {
        Date parsea = simpleDateFormat.parse(timea);
        Date parseb = simpleDateFormat.parse(timeb);
        int result=(int)((parsea.getTime()-parseb.getTime())/(24*60*60*1000));
        return result;

    }
    public static void setBit(byte bytes[],int index,int offset){

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

}
