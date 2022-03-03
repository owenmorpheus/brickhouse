package brickhouse.udf.other;


import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author zhangtinayu02
 * @create 2022/01/10
 * 计算bitmap中符合条件的个数
 * array传入的数组 left right左右的统计范围 0-7 包含 0到7天的值
 * 1-7即最近7天的表现。没有必要写日期。
 * 计算1的个数 left 从0开始。right不能越界。
 * 取数的逻辑，看成数组从左到右递减即可。实际从左到右递增。而且有冗余
 * 为了好合并。能计算再想怎么合并。set
 * 所以给出的left right必须在bitmap标记的范围内。否则也可能有值。都为0
 * 所以新增就是找到位置 置1即可。
 */

public class BitMapCountUDF extends UDF {
    public  static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    public int evaluate(byte[] bitmap,int left ,int right,String st,String et) throws Exception {
        int length = bitmap.length;
        int currentIndex ;
        int currentOffset;
        int result;

        if((datediff(et,st)+1)/8==0){
            currentIndex=length-1;
            currentOffset=7;
        }else{
            int yushu =(datediff(et,st)+1)%8;
            currentIndex=length-1;
            currentOffset=yushu-1;

        }

        String leftloc = preIndexOffset(currentIndex, currentOffset,left);
        String rightloc=preIndexOffset(currentIndex,currentOffset,right);

        result=countIinGap(bitmap,leftloc,rightloc);
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
     * @param bitmap 需要统计的bitmap
     * @param leftloc 逻辑左坐标
     * @param rightloc 逻辑右坐标
     * @return
     */
    public static int countIinGap(byte[] bitmap,String leftloc,String rightloc){

        int result=0;
        String[] split = leftloc.split(",");
        int leftIndex=Integer.valueOf(split[0]);
        int leftOffset=Integer.valueOf(split[1]);
        String[] split1 = rightloc.split(",");
        int rightIndex=Integer.valueOf(split1[0]);
        int rightOffset=Integer.valueOf(split1[1]);
        while (leftIndex>=rightIndex&leftOffset>=rightOffset){
            if(haveIorNot(rightOffset,rightIndex,bitmap)){
                result++;
            }
            if(rightOffset<7){
                rightOffset++;
            }else{
                rightIndex++;
                rightOffset=0;
            }

        }
        return result;
    }
    public static boolean haveIorNot(int offset,int index,byte[] bytes){
        switch (offset) {
            case 0:
                return (byte)(bytes[index]&0x01)!=0;//2^0
            case 1:
                return (byte)(bytes[index]&0x02)!=0;
            case 2:
                return (byte)(bytes[index]&0x04)!=0;
            case 3:
                return (byte)(bytes[index]&0x08)!=0;
            case 4:
                return (byte)(bytes[index]&0x10)!=0;
            case 5:
                return (byte)(bytes[index]&0x20)!=0;
            case 6:
                return (byte)(bytes[index]&0x40)!=0;
            case 7:
                return (byte)(bytes[index]&0x80)!=0;
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
            int zhengshu=leftOrRight/8;
            int yushu = leftOrRight%8;
            int resultIndex=currentIndex-zhengshu;
            int resultOffset;
            if(yushu>currentOffset){
                resultIndex=resultIndex-1;
                resultOffset=8+currentOffset-yushu;
            }else{
                resultOffset=currentOffset-yushu;
            }
            return resultIndex +","+ resultOffset;
        }

    }



    /**
     * 报废了
     * @param input
     * @param condition
     * @return
     * @throws Exception
     */


}
