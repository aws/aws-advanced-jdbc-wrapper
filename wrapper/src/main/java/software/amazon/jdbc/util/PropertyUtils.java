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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

public class PropertyUtils {
  private static final Logger LOGGER = Logger.getLogger(PropertyUtils.class.getName());

  public static void applyProperties(final Object target, final Properties properties) {
    if (target == null || properties == null) {
      return;
    }

    final List<Method> methods = Arrays.asList(target.getClass().getMethods());
    final Enumeration<?> propertyNames = properties.propertyNames();
    while (propertyNames.hasMoreElements()) {
      final Object key = propertyNames.nextElement();
      final String propName = key.toString();
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

    for (final Method method : methods) {
      if (method.getName().equals(methodName) && method.getParameterTypes().length == 1) {
        writeMethod = method;
        break;
      }
    }

    if (writeMethod == null) {
      methodName = "set" + propName.toUpperCase();
      for (final Method method : methods) {
        if (method.getName().equals(methodName) && method.getParameterTypes().length == 1) {
          writeMethod = method;
          break;
        }
      }
    }

    if (writeMethod == null) {
      LOGGER.finest(
          () ->
              Messages.get(
                  "PropertyUtils.setMethodDoesNotExistOnTarget",
                  new Object[] {propName, target.getClass()}));
      return;
    }

    try {
      final Class<?> paramClass = writeMethod.getParameterTypes()[0];
      if (paramClass == String.class) {
        writeMethod.invoke(target, propValue.toString());
      } else if (paramClass == int.class) {
        writeMethod.invoke(target, Integer.parseInt(propValue.toString()));
      } else if (paramClass == long.class) {
        writeMethod.invoke(target, Long.parseLong(propValue.toString()));
      } else if (paramClass == boolean.class || paramClass == Boolean.class) {
        writeMethod.invoke(target, Boolean.parseBoolean(propValue.toString()));
      } else {
        writeMethod.invoke(target, propValue);
      }
      LOGGER.finest(() -> String.format("Set property '%s' with value: %s", propName, propValue));

    } catch (final InvocationTargetException ex) {
      LOGGER.warning(
          () ->
              Messages.get(
                  "PropertyUtils.failedToSetPropertyWithReason",
                  new Object[] {propName, target.getClass(), ex.getCause().getMessage()}));
      throw new RuntimeException(ex.getCause());
    } catch (final Exception e) {
      LOGGER.warning(
          () ->
              Messages.get(
                  "PropertyUtils.failedToSetProperty", new Object[] {propName, target.getClass()}));
      throw new RuntimeException(e);
    }
  }

  public static @NonNull Properties copyProperties(final Properties props) {
    final Properties copy = new Properties();

    if (props == null) {
      return copy;
    }

    for (final Map.Entry<Object, Object> entry : props.entrySet()) {
      copy.setProperty(entry.getKey().toString(), entry.getValue().toString());
    }
    return copy;
  }

  public static String logProperties(final Properties props, final String caption) {
    final StringBuilder sb = new StringBuilder();
    for (final Object key : props.keySet()) {
      if (sb.length() > 0) {
        sb.append("\n");
      }
      sb.append("[").append(key.toString()).append("] ").append(props.get(key).toString());
    }
    if (sb.length() == 0) {
      sb.append("<empty>");
    }
    if (caption != null) {
      sb.insert(0, caption);
    }
    return sb.toString();
  }
}
