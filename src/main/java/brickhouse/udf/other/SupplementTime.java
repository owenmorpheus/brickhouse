package brickhouse.udf.other;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Set;

/**
 * @author 张天宇02
 * @create 2022/02/08
 */
public class SupplementTime extends UDF {
    public String evaluate(String json,String st,String et) throws ParseException {

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Calendar apcalendar =Calendar.getInstance();
        Date apdate = simpleDateFormat.parse(et);
//        Date date = simpleDateFormat.parse(et);
//        calendar.setTime(date);
//        calendar.add(Calendar.DATE,1);
//        System.out.println(simpleDateFormat.format(calendar.getTime()));
//
//        String s = simpleDateFormat.format(calendar.getTime());
//        System.out.println(s);

        JSONObject object = JSONObject.parseObject(json);
        StringBuffer sb = new StringBuffer("");
        String apet="";
        int result=0;
        while (et.compareTo(st)>=0){
            apdate = simpleDateFormat.parse(et);
            apcalendar.setTime(apdate);
            apcalendar.add(Calendar.DATE,-1);
            apet=simpleDateFormat.format(apcalendar.getTime());
            if(object.containsKey(et)){
                result=(int)object.get(et)+result;
            }
            if(result!=0){
                sb.append(apet+":"+result+",");
            }
            et=apet;
        }
        return sb.toString().substring(0,sb.length()-1);
    }


}
