package brickhouse.udf.httputil;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.UDF;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

/**
 * @author zhangtianyu02
 * @create 2022/02/11
 */
public class HttpsUDF extends UDF {
    public static HttpsURLConnection conn = null;
    public String evaluate(String bossid,String jobid) throws InterruptedException, NoSuchAlgorithmException, KeyManagementException {


        String conclusion="";
        String status="";

//        Random r = new Random();
//        int i1 = r.nextInt(5);
////        Thread.currentThread().sleep(10*i1);
        String url="https://sandbox.weizhipin.com/api/abSandbox/common/getCaseConclusion?userId="+bossid+"&jepId="+jobid;
        String json = httpsGet(url);
        JSONObject object = JSONObject.parseObject(json);
        while (object == null){
              Thread.currentThread().sleep(500);
            json = httpsGet(url);
            object = JSONObject.parseObject(json);
        }

        if((int)object.get("code")==200){
            JSONObject obj2=JSONObject.parseObject(object.get("data").toString());
             conclusion= obj2.get("conclusion").toString();
             status = obj2.get("status").toString();
        }

        return conclusion+","+status;



    }

    public static String httpsGet(String url){
        String str_return = "";
        conn = null;
        try {
            SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(null, new TrustManager[] { new TrustAnyTrustManager() },
                    new java.security.SecureRandom());
            URL console = new URL(url);
            conn = (HttpsURLConnection) console.openConnection();
            conn.setSSLSocketFactory(sc.getSocketFactory());
            conn.setHostnameVerifier(new TrustAnyHostnameVerifier());
            conn.connect();
            InputStream is = conn.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(is,
                    ("ISO-8859-1")));
            String ret = "";
            while (ret != null) {
                ret = br.readLine();
                if (ret != null && !ret.trim().equals("")) {
                    str_return = str_return
                            + new String(ret.getBytes("ISO-8859-1"), "utf-8");
                }
            }
        } catch (ConnectException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (KeyManagementException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

            conn.disconnect();
        }
        return str_return;
    }

}
