/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package software.amazon.jdbc.plugin.federatedauth;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

/**
 * Provide a SSLSocketFactory that allows SSL connections to be made without validating the server's
 * certificate. This is more convenient for some applications, but is less secure as it allows "man
 * in the middle" attacks.
 */
public class NonValidatingSSLSocketFactory extends SSLSocketFactory {

  /**
   * We provide a constructor that takes an unused argument solely because the ssl calling code will
   * look for this constructor first and then fall back to the no argument constructor, so we avoid
   * an exception and additional reflection lookups.
   *
   * @param arg input argument
   * @throws GeneralSecurityException if something goes wrong
   */
  public NonValidatingSSLSocketFactory(final String arg) throws GeneralSecurityException {
    final SSLContext ctx = SSLContext.getInstance("TLS"); // or "SSL" ?

    ctx.init(null, new TrustManager[]{new NonValidatingTrustManager()}, null);

    factory = ctx.getSocketFactory();
  }

  protected SSLSocketFactory factory;

  public Socket createSocket(final InetAddress host, final int port) throws IOException {
    return factory.createSocket(host, port);
  }

  public Socket createSocket(final String host, final int port) throws IOException {
    return factory.createSocket(host, port);
  }

  public Socket createSocket(final String host, final int port, final InetAddress localHost, final int localPort)
      throws IOException {
    return factory.createSocket(host, port, localHost, localPort);
  }

  public Socket createSocket(final InetAddress address, final int port, final InetAddress localAddress,
      final int localPort)
      throws IOException {
    return factory.createSocket(address, port, localAddress, localPort);
  }

  public Socket createSocket(final Socket socket, final String host, final int port, final boolean autoClose)
      throws IOException {
    return factory.createSocket(socket, host, port, autoClose);
  }

  public String[] getDefaultCipherSuites() {
    return factory.getDefaultCipherSuites();
  }

  public String[] getSupportedCipherSuites() {
    return factory.getSupportedCipherSuites();
  }

  public static class NonValidatingTrustManager implements X509TrustManager {

    public X509Certificate[] getAcceptedIssuers() {
      return new X509Certificate[0];
    }

    public void checkClientTrusted(final X509Certificate[] certs, final String authType) {
    }

    public void checkServerTrusted(final X509Certificate[] certs, final String authType) {
    }
  }
}
