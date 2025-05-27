package software.amazon.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * A simple utility class to load environment variables from a .env file.
 */
public class EnvLoader {
  private final Map<String, String> envVars = new HashMap<>();

  /**
   * Loads environment variables from a .env file in the current directory.
   */
  public EnvLoader() {
    this(Paths.get(".env"));
  }

  /**
   * Loads environment variables from the specified file path.
   *
   * @param envPath Path to the .env file
   */
  public EnvLoader(Path envPath) {
    if (Files.exists(envPath)) {
      try (BufferedReader reader = new BufferedReader(new FileReader(envPath.toFile()))) {
        String line;
        while ((line = reader.readLine()) != null) {
          parseLine(line);
        }
      } catch (IOException e) {
        System.err.println("Error reading .env file: " + e.getMessage());
      }
    }
  }

  private void parseLine(String line) {
    line = line.trim();
    // Skip empty lines and comments
    if (line.isEmpty() || line.startsWith("#")) {
      return;
    }

    // Split on the first equals sign
    int delimiterPos = line.indexOf('=');
    if (delimiterPos > 0) {
      String key = line.substring(0, delimiterPos).trim();
      String value = line.substring(delimiterPos + 1).trim();
      
      // Remove quotes if present
      if ((value.startsWith("\"") && value.endsWith("\"")) || 
          (value.startsWith("'") && value.endsWith("'"))) {
        value = value.substring(1, value.length() - 1);
      }
      
      envVars.put(key, value);
    }
  }

  /**
   * Gets the value of an environment variable.
   *
   * @param key The name of the environment variable
   * @return The value of the environment variable, or null if not found
   */
  public String get(String key) {
    // First check the loaded .env file
    String value = envVars.get(key);
    
    // If not found, check system environment variables
    if (value == null) {
      value = System.getenv(key);
    }
    
    return value;
  }
}
