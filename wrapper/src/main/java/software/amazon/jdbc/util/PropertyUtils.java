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

package software.amazon.jdbc.util;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.PropertyDefinition;

public class PropertyUtils {
  private static final Logger LOGGER = Logger.getLogger(PropertyUtils.class.getName());

  public static void applyProperties(final Object target, final Properties properties) {
    if (target == null || properties == null) {
      return;
    }

    List<Method> methods = Arrays.asList(target.getClass().getMethods());
    Enumeration<?> propertyNames = properties.propertyNames();
    while (propertyNames.hasMoreElements()) {
      Object key = propertyNames.nextElement();
      String propName = key.toString();
      Object propValue = properties.getProperty(propName);
      if (propValue == null) {
        propValue = properties.get(key);
      }

      setPropertyOnTarget(target, propName, propValue, methods);
    }
  }

  public static void setPropertyOnTarget(
      final Object target,
      final String propName,
      final Object propValue,
      final List<Method> methods) {
    Method writeMethod = null;
    String methodName = "set" + propName.substring(0, 1).toUpperCase() + propName.substring(1);

    for (Method method : methods) {
      if (method.getName().equals(methodName) && method.getParameterTypes().length == 1) {
        writeMethod = method;
        break;
      }
    }

    if (writeMethod == null) {
      methodName = "set" + propName.toUpperCase();
      for (Method method : methods) {
        if (method.getName().equals(methodName) && method.getParameterTypes().length == 1) {
          writeMethod = method;
          break;
        }
      }
    }

    if (writeMethod == null) {
      LOGGER.finest(
          () -> Messages.get(
              "PropertyUtils.setMethodDoesNotExistOnTarget",
              new Object[] {propName, target.getClass()}));
      return;
    }

    try {
      Class<?> paramClass = writeMethod.getParameterTypes()[0];
      if (paramClass == int.class) {
        writeMethod.invoke(target, Integer.parseInt(propValue.toString()));
      } else if (paramClass == long.class) {
        writeMethod.invoke(target, Long.parseLong(propValue.toString()));
      } else if (paramClass == boolean.class || paramClass == Boolean.class) {
        writeMethod.invoke(target, Boolean.parseBoolean(propValue.toString()));
      } else if (paramClass == String.class) {
        writeMethod.invoke(target, propValue.toString());
      } else {
        writeMethod.invoke(target, propValue);
      }
    } catch (Exception e) {
      LOGGER.warning(
          () -> Messages.get(
              "PropertyUtils.failedToSetProperty",
              new Object[] {propName, target.getClass()}));
    }
  }

  public static @NonNull Properties copyProperties(final Properties props) {
    Properties copy = new Properties();

    if (props == null) {
      return copy;
    }

    for (Map.Entry<Object, Object> entry : props.entrySet()) {
      copy.setProperty(entry.getKey().toString(), entry.getValue().toString());
    }
    return copy;
  }

  @SuppressWarnings("checkstyle:LocalVariableName")
  public static @Nullable Properties parseProperties(String url, @Nullable Properties defaults) {

    String urlServer = url;
    String urlArgs = "";

    int qPos = url.indexOf('?');
    if (qPos != -1) {
      urlServer = url.substring(0, qPos);
      urlArgs = url.substring(qPos + 1);
    }

    Properties propertiesFromUrl = new Properties();

    // TODO: allow user to disable database parsing
    int protocolPos = urlServer.indexOf("//");
    if (protocolPos != -1) {
      protocolPos += 2;
    }
    int dPos = urlServer.indexOf("/", protocolPos);
    if (dPos != -1) {
      String database = urlServer.substring(dPos + 1);
      if (!database.isEmpty()) {
        PropertyDefinition.DATABASE.set(propertiesFromUrl, database);
      }
    }

    String[] args = urlArgs.split("&");

    for (String token : args) {

      if (token.isEmpty()) {
        continue;
      }
      int pos = token.indexOf('=');
      if (pos == -1) {
        propertiesFromUrl.setProperty(token, "");
      } else {
        String pName = token.substring(0, pos);
        String pValue = urlDecode(token.substring(pos + 1));
        if (pValue == null) {
          return null;
        }
        propertiesFromUrl.setProperty(pName, pValue);
      }
    }

    Properties result = new Properties();
    result.putAll(propertiesFromUrl);
    if (defaults != null) {
      defaults.forEach(result::putIfAbsent);
    }

    return result;
  }

  private static @Nullable String urlDecode(String url) {
    try {
      return StringUtils.decode(url);
    } catch (IllegalArgumentException e) {
      LOGGER.fine(
          () -> Messages.get(
              "Driver.urlParsingFailed",
              new Object[] {url, e.getMessage()}));
    }
    return null;
  }
}
