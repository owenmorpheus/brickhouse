package brickhouse.udf.other;

import com.kenai.jaffl.annotations.In;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author zhangtianyu02
 * @create 2022/01/17
 */
public class BitmapIncrementLeft extends GenericUDF {
    public  static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableIntObjectInspector);

    }

    /**
     *
     * @param deferredObjects fill填充值。填充1还是0 st为初始化日期,et为etl_date
     * @return
     * @throws HiveException
     */
    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        List<IntWritable> bitmap1= (List<IntWritable>) deferredObjects[0].get();
        List<Integer> bitmap = new ArrayList<>();
        for(IntWritable x:bitmap1){
            bitmap.add(x.get());
        }
        String st=deferredObjects[1].get().toString();
        String et=deferredObjects[2].get().toString();
        int fill= ((IntWritable) deferredObjects[3].get()).get();

        try {
            if((datediff(et,st)+1)==(bitmap.size()*32)+1){
                if(fill==1){
                    bitmap.add(1);
                }else{
                    bitmap.add(0);
                }

            }else if((datediff(et,st)+1)>bitmap.size()*32+1){
                throw new HiveException("之前日期有未初始化的");

            }else {
                if(fill==1){
                    int number=datediff(et,st)+1;
                    setBit(bitmap,number);
                }

            }


        } catch (ParseException e) {
            e.printStackTrace();
        }

        ArrayList<IntWritable> result = new ArrayList<IntWritable>();
        for(Integer x:bitmap){
            result.add(new IntWritable(x));
        }
        return result;


    }

    @Override
    public String getDisplayString(String[] strings) {
        return null;
    }

    public static void setBit(List<Integer> list,int number){
        int index;
        int offset;

        if (0 == number % 32) {
            index = number / 32 - 1;
            offset = 31;
        } else {
            index = number / 32;
            offset = number % 32 - 1;
        }
        switch (offset) {
            case 0:
                list.set(index,list.get(index) | 0x01);
                break;
            case 1:
                list.set(index,list.get(index) | 0x02);
                break;
            case 2:
                list.set(index,list.get(index) | 0x04);
                break;
            case 3:
                list.set(index,list.get(index) | 0x08);
                break;
            case 4:
                list.set(index,list.get(index) | 0x10);
                break;
            case 5:
                list.set(index,list.get(index) | 0x20);
                break;
            case 6:
                list.set(index,list.get(index) | 0x40);
                break;
            case 7:
                list.set(index,list.get(index) | 0x80);
                break;
            case 8:
                list.set(index,list.get(index) | 0x100);
                break;
            case 9:
                list.set(index,list.get(index) | 0x200);
                break;
            case 10:
                list.set(index,list.get(index) | 0x400);
                break;
            case 11:
                list.set(index,list.get(index) | 0x800);
                break;
            case 12:
                list.set(index,list.get(index) | 0x1000);
//                bytes[index] = (byte) (bytes[index] | 0x1000);
                break;
            case 13:
                list.set(index,list.get(index) | 0x2000);
//                bytes[index] = (byte) (bytes[index] | 0x2000);
                break;
            case 14:
                list.set(index,list.get(index) | 0x4000);
//                bytes[index] = (byte) (bytes[index] | 0x4000);
                break;
            case 15:
                list.set(index,list.get(index) | 0x8000);
//                bytes[index] = (byte) (bytes[index] | 0x8000);
                break;
            case 16:
                list.set(index,list.get(index) | 0x10000);
//                bytes[index] = (byte) (bytes[index] | 0x10000);
                break;
            case 17:
                list.set(index,list.get(index) | 0x20000);
//                bytes[index] = (byte) (bytes[index] | 0x20000);
                break;
            case 18:
                list.set(index,list.get(index) | 0x40000);
//                bytes[index] = (byte) (bytes[index] | 0x40000);
                break;
            case 19:
                list.set(index,list.get(index) | 0x80000);
//                bytes[index] = (byte) (bytes[index] | 0x80000);
                break;
            case 20:
                list.set(index,list.get(index) | 0x100000);
//                bytes[index] = (byte) (bytes[index] | 0x100000);
                break;
            case 21:
                list.set(index,list.get(index) | 0x200000);
//                bytes[index] = (byte) (bytes[index] | 0x200000);
                break;
            case 22:
                list.set(index,list.get(index) | 0x400000);
                break;
            case 23:
                list.set(index,list.get(index) | 0x800000);
                break;
            case 24:
                list.set(index,list.get(index) | 0x1000000);
                break;
            case 25:
                list.set(index,list.get(index) | 0x2000000);
                break;
            case 26:
                list.set(index,list.get(index) | 0x4000000);
                break;
            case 27:
                list.set(index,list.get(index) | 0x8000000);
                break;
            case 28:
                list.set(index,list.get(index) | 0x10000000);
                break;
            case 29:
                list.set(index,list.get(index) | 0x20000000);
                break;
            case 30:
                list.set(index,list.get(index) | 0x40000000);
                break;
            case 31:
                list.set(index,list.get(index) | 0x80000000);
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
