package brickhouse.udf.httputil;

/**
 * @author zhangtianyu02
 * @create 2022/02/11
 */
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.X509TrustManager;
public class TrustAnyTrustManager implements X509TrustManager {
    public void checkClientTrusted(X509Certificate[] arg0, String arg1)
            throws CertificateException {
        // TODO Auto-generated method stub
    }
    public void checkServerTrusted(X509Certificate[] arg0, String arg1)
            throws CertificateException {
        // TODO Auto-generated method stub
    }
    public X509Certificate[] getAcceptedIssuers() {
        return new X509Certificate[] {};
    }
}
