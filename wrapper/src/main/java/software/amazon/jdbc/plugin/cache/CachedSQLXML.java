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

import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stax.StAXSource;
import javax.xml.transform.stream.StreamSource;
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
import software.amazon.jdbc.util.Messages;

public class CachedSQLXML implements SQLXML, Serializable {
  private boolean freed;
  private String data;

  public CachedSQLXML(String data) {
    this.data = data;
    this.freed = false;
  }

  @Override
  public void free() throws SQLException {
    if (this.freed) return;
    this.data = null;
    this.freed = true;
  }

  private void checkFreed() throws SQLException {
    if (this.freed) {
      throw new SQLException(Messages.get("CachedSQLXML.alreadyFreed"));
    }
  }

  @Override
  public InputStream getBinaryStream() throws SQLException {
    checkFreed();
    if (this.data == null) return null;
    return new ByteArrayInputStream(this.data.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public OutputStream setBinaryStream() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Reader getCharacterStream() throws SQLException {
    checkFreed();
    if (this.data == null) return null;
    return new StringReader(this.data);
  }

  @Override
  public Writer setCharacterStream() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getString() throws SQLException {
    checkFreed();
    return this.data;
  }

  @Override
  public void setString(String value) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T extends Source> T getSource(Class<T> sourceClass) throws SQLException {
    checkFreed();
    if (this.data == null) return null;

    try {
      if (sourceClass == null || DOMSource.class.equals(sourceClass)) {
        DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        return (T) new DOMSource(builder.parse(new InputSource(new StringReader(data))));
      }

      if (SAXSource.class.equals(sourceClass)) {
        XMLReader reader = XMLReaderFactory.createXMLReader();
        return sourceClass.cast(new SAXSource(reader, new InputSource(new StringReader(data))));
      }

      if (StreamSource.class.equals(sourceClass)) {
        return sourceClass.cast(new StreamSource(new StringReader(data)));
      }

      if (StAXSource.class.equals(sourceClass)) {
        XMLStreamReader xsr = XMLInputFactory.newFactory().createXMLStreamReader(new StringReader(data));
        return sourceClass.cast(new StAXSource(xsr));
      }
      throw new SQLException(Messages.get("CachedSQLXML.unsupportedSourceClass", new Object[] {sourceClass.getName()}));
    } catch (Exception e) {
      throw new SQLException(Messages.get("CachedSQLXML.unableToDecodeXml"), e);
    }
  }

  @Override
  public <T extends Result> T setResult(Class<T> resultClass) throws SQLException {
    throw new UnsupportedOperationException();
  }
}
