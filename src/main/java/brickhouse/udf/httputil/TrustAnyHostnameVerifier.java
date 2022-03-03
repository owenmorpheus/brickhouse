package brickhouse.udf.httputil;

/**
 * @author zhangtianyu02
 * @create 2022/02/11
 */
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;
public class TrustAnyHostnameVerifier implements HostnameVerifier {
    public boolean verify(String arg0, SSLSession arg1) {
        return true;
    }
}
