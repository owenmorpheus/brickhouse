package brickhouse.udf.other;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * @author zhangtianyu02
 * @create 2022/01/13
 */
public class BitmapCount1UDF extends UDF {
    public  static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    public int evaluate(List<Integer> list, int left , int right, String st, String et) throws Exception {
        if(left<0|right<0|left>right){
            throw new Exception("输入数值有误");
        }
        int length = list.size();
        int currentIndex ;
        int currentOffset;
        int result;

        if((datediff(et,st)+1)/32==0){
            currentIndex=length-1;
            currentOffset=31;
        }else{
            int yushu =(datediff(et,st)+1)%32;
            currentIndex=length-1;
            currentOffset=yushu-1;

        }

        String leftloc = preIndexOffset(currentIndex, currentOffset,left);
        String rightloc=preIndexOffset(currentIndex,currentOffset,right);

        result=countIinGap(list,leftloc,rightloc);
        return result;


    }

    public static int datediff(String timea,String timeb) throws ParseException {
        Date parsea = simpleDateFormat.parse(timea);
        Date parseb = simpleDateFormat.parse(timeb);
        int result=(int)((parsea.getTime()-parseb.getTime())/(24*60*60*1000));
        return result;

    }

    /**
     *
     * @param list 需要统计的bitmap
     * @param leftloc 逻辑左坐标
     * @param rightloc 逻辑右坐标
     * @return
     */
    public static int countIinGap(List<Integer> list,String leftloc,String rightloc){

        int result=0;
        String[] split = leftloc.split(",");
        int leftIndex=Integer.valueOf(split[0]);
        int leftOffset=Integer.valueOf(split[1]);
        String[] split1 = rightloc.split(",");
        int rightIndex=Integer.valueOf(split1[0]);
        int rightOffset=Integer.valueOf(split1[1]);
        while (leftIndex>=rightIndex){

            if(haveIorNot(rightOffset,rightIndex,list)){
                result++;
            }

            if(leftIndex==rightIndex&leftOffset==rightOffset){
                break;
            }
            if(rightOffset<31){
                rightOffset++;
            }else{
                rightIndex++;
                rightOffset=0;
            }

        }
        return result;
    }
    public static boolean haveIorNot(int offset,int index,List<Integer> list){
        switch (offset) {
            case 0:
                return (list.get(index)&0x01)!=0;//2^0
            case 1:
                return (list.get(index)&0x02)!=0;
            case 2:
                return (list.get(index)&0x04)!=0;
            case 3:
                return (list.get(index)&0x08)!=0;
            case 4:
                return (list.get(index)&0x10)!=0;
            case 5:
                return (list.get(index)&0x20)!=0;
            case 6:
                return (list.get(index)&0x40)!=0;
            case 7:
                return (list.get(index)&0x80)!=0;
            case 8:
                return (list.get(index)&0x100)!=0;//2^0
            case 9:
                return (list.get(index)&0x200)!=0;
            case 10:
                return (list.get(index)&0x400)!=0;
            case 11:
                return (list.get(index)&0x800)!=0;
            case 12:
                return (list.get(index)&0x1000)!=0;
            case 13:
                return (list.get(index)&0x2000)!=0;
            case 14:
                return (list.get(index)&0x4000)!=0;
            case 15:
                return (list.get(index)&0x8000)!=0;
            case 16:
                return (list.get(index)&0x10000)!=0;
            case 17:
                return (list.get(index)&0x20000)!=0;
            case 18:
                return (list.get(index)&0x40000)!=0;
            case 19:
                return (list.get(index)&0x80000)!=0;
            case 20:
                return (list.get(index)&0x100000)!=0;
            case 21:
                return (list.get(index)&0x200000)!=0;
            case 22:
                return (list.get(index)&0x400000)!=0;
            case 23:
                return (list.get(index)&0x800000)!=0;
            case 24:
                return (list.get(index)&0x1000000)!=0;
            case 25:
                return (list.get(index)&0x2000000)!=0;
            case 26:
                return (list.get(index)&0x4000000)!=0;
            case 27:
                return (list.get(index)&0x8000000)!=0;
            case 28:
                return (list.get(index)&0x10000000)!=0;
            case 29:
                return (list.get(index)&0x20000000)!=0;
            case 30:
                return (list.get(index)&0x40000000)!=0;
            case 31:
                return (list.get(index)&0x80000000)!=0;

        }
        return false;

    }

    /**
     *
     * @param currentIndex 当前时间所在数组下标
     * @param currentOffset 当前时间所在bit下标
     * @param leftOrRight 向后找多少时间。从当前时间开始计算。
     * @return index,offset 向前寻找后所对应的index和offset
     */
    public static String preIndexOffset(int currentIndex,int currentOffset,int leftOrRight) {

        if (leftOrRight == 0) {

            return currentIndex + "," + currentOffset;
        }else{
            int zhengshu=leftOrRight/32;
            int yushu = leftOrRight%32;
            int resultIndex=currentIndex-zhengshu;
            int resultOffset;
            if(yushu>currentOffset){
                resultIndex=resultIndex-1;
                resultOffset=32+currentOffset-yushu;
            }else{
                resultOffset=currentOffset-yushu;
            }
            if(resultIndex<0){
                resultIndex=0;
                resultOffset=0;
            }
            return resultIndex +","+ resultOffset;
        }

    }



}
