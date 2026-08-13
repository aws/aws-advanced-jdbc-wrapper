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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.io.Reader;
import java.sql.SQLException;
import java.sql.SQLXML;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.Source;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stax.StAXSource;
import javax.xml.transform.stream.StreamSource;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

public class CachedSQLXMLTest {

  @Test
  void test_basic_XML() throws Exception {
    String xml = "<root><element1>Value A</element1><element2>Value B</element2></root>";
    SQLXML sqlxml = new CachedSQLXML(xml);
    assertEquals(xml, sqlxml.getString());

    // Test binary stream
    byte[] array = new byte[100];
    InputStream stream = sqlxml.getBinaryStream();
    assertEquals(xml.length(), stream.available());
    assertTrue(stream.read(array) > 0);
    assertEquals(xml, new String(array, 0, xml.length()));
    stream.close();

    // Test character stream
    char[] chars = new char[100];
    Reader reader = sqlxml.getCharacterStream();
    assertTrue(reader.read(chars) > 0);
    assertEquals(xml, new String(chars, 0, xml.length()));
    reader.close();

    // Test free()
    sqlxml.free();
    assertThrows(SQLException.class, sqlxml::getString);
    assertThrows(SQLException.class, sqlxml::getCharacterStream);
    assertThrows(SQLException.class, sqlxml::getBinaryStream);
    assertThrows(SQLException.class, () -> sqlxml.getSource(DOMSource.class));
  }

  private void validateDOMElement(Document document, String elementName, String elementValue) {
    NodeList elements = document.getElementsByTagName(elementName);
    assertEquals(1, elements.getLength());
    Element element = (Element) elements.item(0);
    assertEquals(elementName, element.getNodeName());
    assertEquals(elementValue, element.getTextContent());
  }

  private void validateSimpleDocument(Document document) {
    Element rootElement = document.getDocumentElement();
    assertEquals("product", rootElement.getNodeName());
    NodeList elements = document.getElementsByTagName("product");
    assertEquals(1, elements.getLength()); // product has 3 elements
    elements = document.getElementsByTagName("specs");
    assertEquals(1, elements.getLength()); // specs has 3 elements
    validateDOMElement(document, "manufacturer", "TechCorp");
    validateDOMElement(document, "cpu", "Intel i7");
    validateDOMElement(document, "ram", "16GB");
    validateDOMElement(document, "storage", "512GB SSD");
    validateDOMElement(document, "price", "1200.00");
  }

  private static void validateDocElements(String name, String value) {
    if (name.equalsIgnoreCase("manufacturer")) {
      assertEquals("TechCorp", value);
    } else if (name.equalsIgnoreCase("cpu")) {
      assertEquals("Intel i7", value);
    } else if (name.equalsIgnoreCase("ram")) {
      assertEquals("16GB", value);
    } else if (name.equalsIgnoreCase("storage")) {
      assertEquals("512GB SSD", value);
    } else if (name.equalsIgnoreCase("price")) {
      assertEquals("1200.00", value);
    }
  }

  private static class XmlReaderContentHandler extends DefaultHandler {
    private StringBuilder currentValue;

    @Override
    public void startElement(String uri, String localName, String qualifiedName, Attributes attributes) {
      currentValue = new StringBuilder(); // Reset for each new element
    }

    @Override
    public void endElement(String uri, String localName, String qualifiedName) {
      // Verify the element's value
      String value = currentValue.toString().trim();
      validateDocElements(qualifiedName, value);
    }

    @Override
    public void characters(char[] ch, int start, int length) {
      currentValue.append(ch, start, length);
    }
  }

  @Test
  void test_getSource_XML() throws Exception {
    // Test parsing a more complex XML via getSource()
    String xml = "    \n"
        + "<product>\n"
        + "        <manufacturer>TechCorp</manufacturer>\n\n"
        + "<specs>\n"
        + "            <cpu>Intel i7</cpu>\n"
        + "            <ram>16GB</ram>\n"
        + "            <storage>512GB SSD</storage>\n"
        + "</specs>\n"
        + "        <price>1200.00</price>\n"
        + "</product>\n";
    SQLXML sqlxml = new CachedSQLXML(xml);
    assertEquals(xml, sqlxml.getString());

    // DOM source
    DOMSource domSource = sqlxml.getSource(null);
    Node node = domSource.getNode();
    assertEquals(Node.DOCUMENT_NODE, node.getNodeType());
    validateSimpleDocument((Document) node);
    domSource = sqlxml.getSource(DOMSource.class);
    node = domSource.getNode();
    assertEquals(Node.DOCUMENT_NODE, node.getNodeType());
    validateSimpleDocument((Document) node);

    // SAX source
    SAXSource src = sqlxml.getSource(SAXSource.class);
    XMLReader xmlReader = src.getXMLReader();
    xmlReader.setContentHandler(new XmlReaderContentHandler());
    xmlReader.parse(src.getInputSource());

    // Stream source is disabled by default; verify it throws, then inject an opt-in config
    // on a fresh instance and verify the passthrough behavior still works.
    assertThrows(SQLException.class, () -> sqlxml.getSource(StreamSource.class));
    CachedSQLXML optedInXml = new CachedSQLXML(xml);
    optedInXml.setDeserializationConfig(new CacheDeserializationConfig(false, true));
    StreamSource xmlSource = optedInXml.getSource(StreamSource.class);
    DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    Document doc = db.parse(new InputSource(xmlSource.getReader()));
    doc.getDocumentElement().normalize();
    validateSimpleDocument(doc);

    // StAX Source
    StAXSource staxSource = sqlxml.getSource(StAXSource.class);
    XMLStreamReader streamReader = staxSource.getXMLStreamReader();
    String elementName = "";
    StringBuilder elementValue = new StringBuilder();
    while (streamReader.hasNext()) {
      int event = streamReader.next();
      if (event == XMLStreamReader.START_ELEMENT) {
        elementName = streamReader.getLocalName();
      } else if (event == XMLStreamReader.CHARACTERS) {
        elementValue.append(streamReader.getText());
      } else if (event == XMLStreamReader.END_ELEMENT) {
        validateDocElements(elementName, elementValue.toString().trim());
        elementName = "";
        elementValue = new StringBuilder();
      }
    }
    streamReader.close(); // Close the reader when done

    // Invalid source class
    assertThrows(SQLException.class, () -> sqlxml.getSource(Source.class));
  }

  // XML value used to verify that DOCTYPE declarations are rejected across all parser branches.
  private static final String XML_WITH_DOCTYPE =
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<!DOCTYPE root [\n"
          + "  <!ENTITY xxe SYSTEM \"file:///etc/hostname\">\n"
          + "]>\n"
          + "<root>&xxe;</root>";

  @Test
  void test_getSource_DOMSource_rejectsDoctype() {
    SQLXML sqlxml = new CachedSQLXML(XML_WITH_DOCTYPE);
    assertThrows(SQLException.class, () -> sqlxml.getSource(DOMSource.class));
    // Default source class (null) also routes to DOMSource.
    assertThrows(SQLException.class, () -> sqlxml.getSource(null));
  }

  @Test
  void test_getSource_SAXSource_rejectsDoctype() throws Exception {
    SQLXML sqlxml = new CachedSQLXML(XML_WITH_DOCTYPE);
    // SAX parses lazily; obtaining the source succeeds, but pumping the reader must throw.
    SAXSource src = sqlxml.getSource(SAXSource.class);
    XMLReader reader = src.getXMLReader();
    reader.setContentHandler(new DefaultHandler());
    assertThrows(Exception.class, () -> reader.parse(src.getInputSource()));
  }

  @Test
  void test_getSource_StAXSource_rejectsDoctype() throws Exception {
    SQLXML sqlxml = new CachedSQLXML(XML_WITH_DOCTYPE);
    // StAX is also lazy; iterating the stream reader must throw.
    StAXSource staxSource = sqlxml.getSource(StAXSource.class);
    XMLStreamReader streamReader = staxSource.getXMLStreamReader();
    assertThrows(Exception.class, () -> {
      while (streamReader.hasNext()) {
        streamReader.next();
      }
    });
  }

  @Test
  void test_getSource_StreamSource_disabledByDefault() {
    // A CachedSQLXML with no injected config falls back to STRICT, so StreamSource is rejected.
    SQLXML sqlxml = new CachedSQLXML("<root/>");
    assertThrows(SQLException.class, () -> sqlxml.getSource(StreamSource.class));
  }

  // Standalone Xerces 2.x predates JAXP 1.5 and rejects the ACCESS_EXTERNAL_DTD / ACCESS_EXTERNAL
  // _SCHEMA properties with IllegalArgumentException (DOM) and SAXNotRecognizedException (SAX).
  // getSource() currently sets those properties fatally, so on such a classpath EVERY document --
  // including benign XML with no DOCTYPE -- fails to parse. These fake factories reproduce that
  // behavior per-test (via the JAXP system property) without pulling xercesImpl onto the whole
  // test classpath, where its META-INF/services entry would repoint every JAXP lookup.
  //
  // Against the current (unfixed) code, assertion (1) in each test fails: getSource throws
  // SQLException for benign XML. Assertion (2) proves the load-bearing disallow-doctype-decl
  // control still rejects DOCTYPE payloads on this classpath.

  @Test
  void test_getSource_DOMSource_standaloneXercesClasspath() throws Exception {
    final String prop = "javax.xml.parsers.DocumentBuilderFactory";
    final String original = System.getProperty(prop);
    // Capture the JDK default BEFORE installing the override (Java 8-safe; avoids recursion).
    XercesLikeDocumentBuilderFactory.delegate = DocumentBuilderFactory.newInstance();
    System.setProperty(prop, XercesLikeDocumentBuilderFactory.class.getName());
    try {
      assertTrue(DocumentBuilderFactory.newInstance() instanceof XercesLikeDocumentBuilderFactory);

      SQLXML benign = new CachedSQLXML("<product><manufacturer>TechCorp</manufacturer></product>");
      DOMSource domSource = benign.getSource(DOMSource.class);
      Node node = domSource.getNode();
      assertEquals(Node.DOCUMENT_NODE, node.getNodeType());
      validateDOMElement((Document) node, "manufacturer", "TechCorp");

      // disallow-doctype-decl is NOT one of the softened properties,
      // so a DOCTYPE payload is still rejected.
      SQLXML malicious = new CachedSQLXML(XML_WITH_DOCTYPE);
      assertThrows(SQLException.class, () -> malicious.getSource(DOMSource.class));
    } finally {
      if (original == null) {
        System.clearProperty(prop);
      } else {
        System.setProperty(prop, original);
      }
      XercesLikeDocumentBuilderFactory.delegate = null;
    }
  }

  @Test
  void test_getSource_SAXSource_standaloneXercesClasspath() throws Exception {
    final String prop = "javax.xml.parsers.SAXParserFactory";
    final String original = System.getProperty(prop);
    XercesLikeSaxParserFactory.delegate = SAXParserFactory.newInstance();
    System.setProperty(prop, XercesLikeSaxParserFactory.class.getName());
    try {
      assertTrue(SAXParserFactory.newInstance() instanceof XercesLikeSaxParserFactory);

      SQLXML benign = new CachedSQLXML("<product><manufacturer>TechCorp</manufacturer></product>");
      SAXSource src = benign.getSource(SAXSource.class);
      XMLReader xmlReader = src.getXMLReader();
      xmlReader.setContentHandler(new DefaultHandler());
      xmlReader.parse(src.getInputSource()); // benign parse must not throw

      // a DOCTYPE payload is rejected when the reader runs.
      SQLXML malicious = new CachedSQLXML(XML_WITH_DOCTYPE);
      SAXSource malSrc = malicious.getSource(SAXSource.class);
      XMLReader malReader = malSrc.getXMLReader();
      malReader.setContentHandler(new DefaultHandler());
      assertThrows(Exception.class, () -> malReader.parse(malSrc.getInputSource()));
    } finally {
      if (original == null) {
        System.clearProperty(prop);
      } else {
        System.setProperty(prop, original);
      }
      XercesLikeSaxParserFactory.delegate = null;
    }
  }

  /**
   * A {@link DocumentBuilderFactory} that mimics standalone Apache Xerces 2.x: it rejects the JAXP
   * 1.5 {@code ACCESS_EXTERNAL_*} attributes and delegates everything else to the JDK default.
   */
  public static final class XercesLikeDocumentBuilderFactory extends DocumentBuilderFactory {
    // Captured from the JDK default BEFORE this factory is installed via the system property.
    static DocumentBuilderFactory delegate;

    // Forward all state to the delegate rather than reading it back via base-class getters:
    // DocumentBuilderFactory.isXIncludeAware()/isExpandEntityReferences() throw
    // UnsupportedOperationException in the abstract base unless a concrete subclass overrides them.
    @Override
    public DocumentBuilder newDocumentBuilder() throws ParserConfigurationException {
      return delegate.newDocumentBuilder();
    }

    @Override
    public void setAttribute(String name, Object value) {
      if (XMLConstants.ACCESS_EXTERNAL_DTD.equals(name)
          || XMLConstants.ACCESS_EXTERNAL_SCHEMA.equals(name)) {
        throw new IllegalArgumentException("Property '" + name + "' is not recognized.");
      }
      delegate.setAttribute(name, value);
    }

    @Override
    public Object getAttribute(String name) {
      return delegate.getAttribute(name);
    }

    @Override
    public void setFeature(String name, boolean value) throws ParserConfigurationException {
      delegate.setFeature(name, value);
    }

    @Override
    public boolean getFeature(String name) throws ParserConfigurationException {
      return delegate.getFeature(name);
    }

    @Override
    public void setNamespaceAware(boolean value) {
      delegate.setNamespaceAware(value);
    }

    @Override
    public void setValidating(boolean value) {
      delegate.setValidating(value);
    }

    @Override
    public void setXIncludeAware(boolean value) {
      delegate.setXIncludeAware(value);
    }

    @Override
    public void setExpandEntityReferences(boolean value) {
      delegate.setExpandEntityReferences(value);
    }
  }

  /**
   * A {@link SAXParserFactory} whose parsers mimic standalone Apache Xerces 2.x: {@code
   * SAXParser.setProperty} rejects the JAXP 1.5 {@code ACCESS_EXTERNAL_*} properties and delegates
   * everything else to the JDK default.
   */
  public static final class XercesLikeSaxParserFactory extends SAXParserFactory {
    static SAXParserFactory delegate;

    @Override
    public SAXParser newSAXParser() throws ParserConfigurationException, SAXException {
      delegate.setNamespaceAware(isNamespaceAware());
      delegate.setValidating(isValidating());
      return new XercesLikeSaxParser(delegate.newSAXParser());
    }

    @Override
    public void setFeature(String name, boolean value)
        throws ParserConfigurationException, SAXNotRecognizedException, SAXNotSupportedException {
      delegate.setFeature(name, value);
    }

    @Override
    public boolean getFeature(String name)
        throws ParserConfigurationException, SAXNotRecognizedException, SAXNotSupportedException {
      return delegate.getFeature(name);
    }
  }

  @SuppressWarnings("deprecation") // org.xml.sax.Parser is a deprecated abstract-method return type.
  static final class XercesLikeSaxParser extends SAXParser {
    private final SAXParser delegate;

    XercesLikeSaxParser(SAXParser delegate) {
      this.delegate = delegate;
    }

    @Override
    public void setProperty(String name, Object value)
        throws SAXNotRecognizedException, SAXNotSupportedException {
      if (XMLConstants.ACCESS_EXTERNAL_DTD.equals(name)
          || XMLConstants.ACCESS_EXTERNAL_SCHEMA.equals(name)) {
        throw new SAXNotRecognizedException("Property '" + name + "' is not recognized.");
      }
      delegate.setProperty(name, value);
    }

    @Override
    public Object getProperty(String name)
        throws SAXNotRecognizedException, SAXNotSupportedException {
      return delegate.getProperty(name);
    }

    @Override
    public org.xml.sax.Parser getParser() throws SAXException {
      return delegate.getParser();
    }

    @Override
    public XMLReader getXMLReader() throws SAXException {
      return delegate.getXMLReader();
    }

    @Override
    public boolean isNamespaceAware() {
      return delegate.isNamespaceAware();
    }

    @Override
    public boolean isValidating() {
      return delegate.isValidating();
    }
  }
}
