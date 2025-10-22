package software.amazon.jdbc.plugin.cache;

import org.junit.jupiter.api.Test;
import org.w3c.dom.*;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import java.io.InputStream;
import java.io.Reader;
import java.sql.SQLException;
import java.sql.SQLXML;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.Source;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stax.StAXSource;
import javax.xml.transform.stream.StreamSource;

import static org.junit.jupiter.api.Assertions.*;

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

  static private void validateDocElements(String name, String value) {
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

  static private class XmlReaderContentHandler extends DefaultHandler {
    private StringBuilder currentValue;

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) {
      currentValue = new StringBuilder(); // Reset for each new element
    }

    @Override
    public void endElement(String uri, String localName, String qName) {
      // Verify the element's value
      String value = currentValue.toString().trim();
      validateDocElements(qName, value);
    }

    @Override
    public void characters(char[] ch, int start, int length) {
      currentValue.append(ch, start, length);
    }
  }

  @Test
  void test_getSource_XML() throws Exception {
    // Test parsing a more complex XML via getSource()
    String xml = "    \n" +
        "<product>\n" +
        "        <manufacturer>TechCorp</manufacturer>\n\n" +
        "<specs>\n" +
        "            <cpu>Intel i7</cpu>\n" +
        "            <ram>16GB</ram>\n" +
        "            <storage>512GB SSD</storage>\n" +
        "</specs>\n" +
        "        <price>1200.00</price>\n" +
        "</product>\n";
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

    // Streams source
    StreamSource xmlSource = sqlxml.getSource(StreamSource.class);
    DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    Document doc = db.parse(new InputSource(xmlSource.getReader()));
    doc.getDocumentElement().normalize();
    validateSimpleDocument(doc);

    // StAX Source
    StAXSource staxSource = sqlxml.getSource(StAXSource.class);
    XMLStreamReader sReader = staxSource.getXMLStreamReader();
    String elementName = "";
    StringBuilder elementValue = new StringBuilder();
    while (sReader.hasNext()) {
      int event = sReader.next();
      if (event == XMLStreamReader.START_ELEMENT) {
        elementName = sReader.getLocalName();
      } else if (event == XMLStreamReader.CHARACTERS) {
        elementValue.append(sReader.getText());
      } else if (event == XMLStreamReader.END_ELEMENT) {
        validateDocElements(elementName, elementValue.toString().trim());
        elementName = "";
        elementValue = new StringBuilder();
      }
    }
    sReader.close(); // Close the reader when done

    // Invalid source class
    assertThrows(SQLException.class, () -> sqlxml.getSource(Source.class));
  }
}
