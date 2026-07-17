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

package software.amazon.jdbc.plugin.cache;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Serializable;
import java.io.StringReader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stax.StAXSource;
import javax.xml.transform.stream.StreamSource;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;
import software.amazon.jdbc.util.Messages;

public class CachedSQLXML implements SQLXML, Serializable {
  // Controls whether getSource(StreamSource.class) is permitted for XML retrieved from the cache.
  // Because a StreamSource returns the XML unparsed and the driver cannot control how the caller
  // subsequently parses it, this path is disabled by default. Users can opt in to the previous
  // passthrough behavior via the remoteQueryCachePlugin.allowStreamSourceFromCache property.
  private static final AtomicBoolean ALLOW_STREAM_SOURCE_FROM_CACHE = new AtomicBoolean(false);

  private boolean freed;
  private @Nullable String data;

  public CachedSQLXML(String data) {
    this.data = data;
    this.freed = false;
  }

  /**
   * Configures whether {@link #getSource(Class)} accepts {@link StreamSource} as a source type.
   * Set by the Remote Query Cache Plugin from the
   * {@code remoteQueryCachePlugin.allowStreamSourceFromCache} property at plugin initialization.
   *
   * @param allow {@code true} to allow returning an unparsed {@code StreamSource}; {@code false}
   *     (the default) to throw {@link SQLException} for {@code StreamSource} requests
   */
  public static void setAllowStreamSourceFromCache(boolean allow) {
    ALLOW_STREAM_SOURCE_FROM_CACHE.set(allow);
  }

  @Override
  public void free() throws SQLException {
    if (this.freed) {
      return;
    }
    this.data = null;
    this.freed = true;
  }

  private void checkFreed() throws SQLException {
    if (this.freed) {
      throw new SQLException(Messages.get("CachedSQLXML.alreadyFreed"));
    }
  }

  @Override
  // "return": returns null when the backing XML data has been freed/absent, preserving prior
  // behavior; the SQLXML.getBinaryStream contract is annotated non-null.
  @SuppressWarnings("return")
  public InputStream getBinaryStream() throws SQLException {
    checkFreed();
    if (this.data == null) {
      return null;
    }
    return new ByteArrayInputStream(this.data.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public OutputStream setBinaryStream() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  // "return": returns null when the backing XML data has been freed/absent, preserving prior
  // behavior; the SQLXML.getCharacterStream contract is annotated non-null.
  @SuppressWarnings("return")
  public Reader getCharacterStream() throws SQLException {
    checkFreed();
    if (this.data == null) {
      return null;
    }
    return new StringReader(this.data);
  }

  @Override
  public Writer setCharacterStream() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  // "return": returns null when the backing XML data has been freed/absent, preserving prior
  // behavior; the SQLXML.getString contract is annotated non-null.
  @SuppressWarnings("return")
  public String getString() throws SQLException {
    checkFreed();
    return this.data;
  }

  @Override
  public void setString(String value) throws SQLException {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns a {@link Source} for reading the XML value represented by this object.
   *
   * <p>For {@link DOMSource}, {@link SAXSource}, and {@link StAXSource}, the XML is parsed by this
   * method using a parser configured to disable DTDs and external entity resolution, following the
   * <a href="https://cheatsheetseries.owasp.org/cheatsheets/XML_External_Entity_Prevention_Cheat_Sheet.html">OWASP
   * XML External Entity Prevention Cheat Sheet</a>.
   *
   * <p>{@link StreamSource} is disabled by default. Because a {@code StreamSource} returns the XML
   * unparsed and the driver cannot control how the caller subsequently parses it, requesting one
   * from a cached {@code SQLXML} throws {@link SQLException}. Callers that require this behavior
   * can opt in by setting the plugin property
   * {@code remoteQueryCachePlugin.allowStreamSourceFromCache=true}, in which case the consumer of
   * the returned {@code StreamSource} is responsible for configuring its own parser or transformer
   * securely (for example, by disabling DTDs and external entities).
   *
   * @param sourceClass the class of the {@code Source} to return, or {@code null} for the default
   *     ({@code DOMSource})
   * @param <T> the type of {@code Source}
   * @return a {@code Source} for reading the XML value, or {@code null} if the value has been freed
   * @throws SQLException if the value has been freed, the XML cannot be decoded, or the requested
   *     source class is unsupported
   */
  @Override
  // "return": getSource legitimately returns null when the backing data has been freed/absent;
  // the generic return type T cannot be annotated @Nullable, so the null return is suppressed.
  @SuppressWarnings({"unchecked", "return"})
  public <T extends Source> T getSource(Class<T> sourceClass) throws SQLException {
    checkFreed();
    final String xmlData = this.data;
    if (xmlData == null) {
      return null;
    }

    try {
      if (sourceClass == null || DOMSource.class.equals(sourceClass)) {
        // Disable DOCTYPE and external entity resolution.
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        dbf.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        dbf.setFeature("http://xml.org/sax/features/external-general-entities", false);
        dbf.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        dbf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
        dbf.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
        dbf.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
        dbf.setXIncludeAware(false);
        dbf.setExpandEntityReferences(false);
        DocumentBuilder builder = dbf.newDocumentBuilder();
        return (T) new DOMSource(builder.parse(new InputSource(new StringReader(xmlData))));
      }

      if (SAXSource.class.equals(sourceClass)) {
        // Disable DOCTYPE and external entity resolution.
        SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        spf.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        spf.setFeature("http://xml.org/sax/features/external-general-entities", false);
        spf.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        SAXParser parser = spf.newSAXParser();
        parser.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, "");
        parser.setProperty(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
        XMLReader reader = parser.getXMLReader();
        return sourceClass.cast(new SAXSource(reader, new InputSource(new StringReader(xmlData))));
      }

      if (StreamSource.class.equals(sourceClass)) {
        if (!ALLOW_STREAM_SOURCE_FROM_CACHE.get()) {
          throw new SQLException(Messages.get("CachedSQLXML.streamSourceDisabled"));
        }
        return sourceClass.cast(new StreamSource(new StringReader(xmlData)));
      }

      if (StAXSource.class.equals(sourceClass)) {
        // Disable DOCTYPE and external entity resolution.
        XMLInputFactory xif = XMLInputFactory.newFactory();
        xif.setProperty(XMLInputFactory.SUPPORT_DTD, false);
        xif.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
        XMLStreamReader xsr = xif.createXMLStreamReader(new StringReader(xmlData));
        return sourceClass.cast(new StAXSource(xsr));
      }
      throw new SQLException(Messages.get("CachedSQLXML.unsupportedSourceClass", new Object[]{sourceClass.getName()}));
    } catch (Exception e) {
      throw new SQLException(Messages.get("CachedSQLXML.unableToDecodeXml"), e);
    }
  }

  @Override
  public <T extends Result> T setResult(Class<T> resultClass) throws SQLException {
    throw new UnsupportedOperationException();
  }
}
