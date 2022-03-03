package brickhouse.udf.other;

import org.apache.hadoop.hive.ql.exec.UDF;
import net.sourceforge.pinyin4j.PinyinHelper;
import net.sourceforge.pinyin4j.format.HanyuPinyinCaseType;
import net.sourceforge.pinyin4j.format.HanyuPinyinOutputFormat;
import net.sourceforge.pinyin4j.format.HanyuPinyinToneType;
import net.sourceforge.pinyin4j.format.exception.BadHanyuPinyinOutputFormatCombination;

/**
 * @author zhangtianyu02
 * @create 2022/01/10
 */
public class HanZiTransform extends UDF {


    public String evaluate(String str)  {
            String result="";
        try {
            if (str == null) {
                return null;
            }
            char[] b = str.toCharArray();
            StringBuffer stringBuffer = new StringBuffer("");
            for (char c : b) {
                char c1 = Character.toLowerCase(c);
                stringBuffer = stringBuffer.append(c1);
            }
            String input = stringBuffer.toString();
            HanyuPinyinOutputFormat format = new HanyuPinyinOutputFormat();
            //拼音小写
            format.setCaseType(HanyuPinyinCaseType.LOWERCASE);
            //不带声调
            format.setToneType(HanyuPinyinToneType.WITHOUT_TONE);

            //要转换的中文，格式，转换之后的拼音的分隔符，遇到不能转换的是否保留   wo,shi,zhong,guo,ren,，hello
             result = PinyinHelper.toHanYuPinyinString(input, format, "", true);


        } catch (BadHanyuPinyinOutputFormatCombination e) {
            System.out.println("字符不能转成汉语拼音");
        }
        return result;
    }


}
