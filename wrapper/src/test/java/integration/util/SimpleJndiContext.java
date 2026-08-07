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

package integration.util;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import javax.naming.Binding;
import javax.naming.Context;
import javax.naming.Name;
import javax.naming.NameClassPair;
import javax.naming.NameParser;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.spi.ObjectFactory;

/**
 * The Context for a simple JNDI implementation. This is not a complete implementation, and is only used for
 * testing purposes. Objects are stored as references.
 */
public class SimpleJndiContext implements Context {
  private final Map<String, Object> map = new HashMap<>();

  public SimpleJndiContext() {
  }

  public Object lookup(Name name) throws NamingException {
    return lookup(name.get(0));
  }

  public Object lookup(String name) throws NamingException {
    Object object = map.get(name);
    if (object == null) {
      return null;
    }

    try {
      Reference ref = (Reference) object;
      Class<?> factoryClass = Class.forName(ref.getFactoryClassName());
      ObjectFactory factory = (ObjectFactory) factoryClass.getDeclaredConstructor().newInstance();
      return factory.getObjectInstance(ref, null, this, null);
    } catch (Exception e) {
      throw new NamingException(e.getMessage());
    }
  }

  public void bind(Name name, Object obj) throws NamingException {
    rebind(name.get(0), obj);
  }

  public void bind(String name, Object obj) throws NamingException {
    rebind(name, obj);
  }

  public void rebind(Name name, Object obj) throws NamingException {
    rebind(name.get(0), obj);
  }

  public void rebind(String name, Object obj) throws NamingException {
    map.put(name, ((Referenceable) obj).getReference());
  }

  public void unbind(Name name) {
    unbind(name.get(0));
  }

  public void unbind(String name) {
    map.remove(name);
  }

  public void rename(Name oldName, Name newName) {
    rename(oldName.get(0), newName.get(0));
  }

  public void rename(String oldName, String newName) {
    map.put(newName, map.remove(oldName));
  }

  public NamingEnumeration<NameClassPair> list(Name name) {
    return null;
  }

  public NamingEnumeration<NameClassPair> list(String name) {
    return null;
  }

  public NamingEnumeration<Binding> listBindings(Name name) {
    return null;
  }

  public NamingEnumeration<Binding> listBindings(String name) {
    return null;
  }

  public void destroySubcontext(Name name) {
  }

  public void destroySubcontext(String name) {
  }

  public Context createSubcontext(Name name) {
    return null;
  }

  public Context createSubcontext(String name) {
    return null;
  }

  public Object lookupLink(Name name) {
    return null;
  }

  public Object lookupLink(String name) {
    return null;
  }

  public NameParser getNameParser(Name name) {
    return null;
  }

  public NameParser getNameParser(String name) {
    return null;
  }

  public Name composeName(Name name, Name prefix) {
    return null;
  }

  public String composeName(String name, String prefix) {
    return null;
  }

  public Object addToEnvironment(String propName, Object propVal) {
    return null;
  }

  public Object removeFromEnvironment(String propName) {
    return null;
  }

  public Hashtable<?, ?> getEnvironment() {
    return null;
  }

  public void close() {
  }

  public String getNameInNamespace() {
    return null;
  }
}
