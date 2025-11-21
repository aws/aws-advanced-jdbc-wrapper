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
      throw new SQLException("This SQLXML object has already been freed.");
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
      throw new SQLException("Unsupported source class for XML data: " + sourceClass.getName());
    } catch (Exception e) {
      throw new SQLException("Unable to decode XML data.", e);
    }
  }

  @Override
  public <T extends Result> T setResult(Class<T> resultClass) throws SQLException {
    throw new UnsupportedOperationException();
  }
}
