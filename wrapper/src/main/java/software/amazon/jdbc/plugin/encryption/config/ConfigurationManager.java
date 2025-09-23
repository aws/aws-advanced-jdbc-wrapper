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


package software.amazon.jdbc.plugin.encryption.config;

import software.amazon.jdbc.plugin.encryption.model.EncryptionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.*;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Manages configuration loading and hot-reloading capabilities.
 * Monitors configuration files for changes and notifies listeners when updates occur.
 */
public class ConfigurationManager {

    private static final Logger logger = LoggerFactory.getLogger(ConfigurationManager.class);

    private final AtomicReference<EncryptionConfig> currentConfig;
    private final ScheduledExecutorService watcherExecutor;
    private final Path configFilePath;
    private volatile WatchService watchService;
    private volatile Instant lastModified;
    private volatile Consumer<EncryptionConfig> configChangeListener;

    public ConfigurationManager(EncryptionConfig initialConfig) {
        this(initialConfig, null);
    }

    public ConfigurationManager(EncryptionConfig initialConfig, String configFilePath) {
        this.currentConfig = new AtomicReference<>(initialConfig);
        this.configFilePath = configFilePath != null ? Paths.get(configFilePath) : null;
        this.lastModified = Instant.EPOCH;
        this.watcherExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ConfigurationManager-Watcher");
            t.setDaemon(true);
            return t;
        });

        if (initialConfig.isHotReloadEnabled() && this.configFilePath != null) {
            startFileWatcher();
        }

        logger.info("ConfigurationManager initialized, hotReload={}, configFile={}",
                   initialConfig.isHotReloadEnabled(), configFilePath);
    }

    /**
     * Gets the current configuration.
     */
    public EncryptionConfig getCurrentConfig() {
        return currentConfig.get();
    }

    /**
     * Updates the configuration and notifies listeners.
     */
    public void updateConfig(EncryptionConfig newConfig) {
        EncryptionConfig oldConfig = currentConfig.getAndSet(newConfig);

        logger.info("Configuration updated");

        // Restart file watcher if hot reload setting changed
        if (oldConfig.isHotReloadEnabled() != newConfig.isHotReloadEnabled()) {
            if (newConfig.isHotReloadEnabled() && configFilePath != null) {
                startFileWatcher();
            } else {
                stopFileWatcher();
            }
        }

        // Notify listener if registered
        if (configChangeListener != null) {
            try {
                configChangeListener.accept(newConfig);
            } catch (Exception e) {
                logger.error("Error notifying configuration change listener", e);
            }
        }
    }

    /**
     * Sets a listener to be notified when configuration changes.
     */
    public void setConfigChangeListener(Consumer<EncryptionConfig> listener) {
        this.configChangeListener = listener;
    }

    /**
     * Loads configuration from a properties file.
     */
    public static EncryptionConfig loadFromFile(String filePath) throws IOException {
        Properties properties = new Properties();

        try (InputStream input = Files.newInputStream(Paths.get(filePath))) {
            properties.load(input);
            logger.info("Loaded configuration from file: {}", filePath);
            return EncryptionConfig.fromProperties(properties);
        }
    }

    /**
     * Loads configuration from classpath resource.
     */
    public static EncryptionConfig loadFromResource(String resourcePath) throws IOException {
        Properties properties = new Properties();

        try (InputStream input = ConfigurationManager.class.getClassLoader().getResourceAsStream(resourcePath)) {
            if (input == null) {
                throw new IOException("Configuration resource not found: " + resourcePath);
            }
            properties.load(input);
            logger.info("Loaded configuration from resource: {}", resourcePath);
            return EncryptionConfig.fromProperties(properties);
        }
    }

    /**
     * Reloads configuration from the configured file path.
     */
    public void reloadFromFile() {
        if (configFilePath == null) {
            logger.warn("Cannot reload configuration - no file path configured");
            return;
        }

        try {
            EncryptionConfig newConfig = loadFromFile(configFilePath.toString());
            updateConfig(newConfig);
            logger.info("Configuration reloaded from file: {}", configFilePath);
        } catch (Exception e) {
            logger.error("Failed to reload configuration from file: {}", configFilePath, e);
        }
    }

    /**
     * Validates that a configuration change is safe to apply.
     */
    public boolean validateConfigChange(EncryptionConfig newConfig) {
        try {
            newConfig.validate();

            EncryptionConfig current = currentConfig.get();

            // Check for potentially dangerous changes
            if (!current.getKmsRegion().equals(newConfig.getKmsRegion())) {
                logger.warn("Configuration change includes KMS region change: {} -> {}",
                           current.getKmsRegion(), newConfig.getKmsRegion());
            }

            if (current.getKmsConnectionPoolSize() != newConfig.getKmsConnectionPoolSize()) {
                logger.info("KMS connection pool size changing: {} -> {}",
                           current.getKmsConnectionPoolSize(), newConfig.getKmsConnectionPoolSize());
            }

            return true;

        } catch (Exception e) {
            logger.error("Configuration validation failed", e);
            return false;
        }
    }

    /**
     * Shuts down the configuration manager.
     */
    public void shutdown() {
        logger.info("Shutting down ConfigurationManager");

        stopFileWatcher();

        watcherExecutor.shutdown();
        try {
            if (!watcherExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                watcherExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            watcherExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Starts the file watcher for hot-reloading.
     */
    private void startFileWatcher() {
        if (configFilePath == null || !Files.exists(configFilePath)) {
            logger.warn("Cannot start file watcher - config file does not exist: {}", configFilePath);
            return;
        }

        stopFileWatcher(); // Stop any existing watcher

        try {
            watchService = FileSystems.getDefault().newWatchService();
            Path parentDir = configFilePath.getParent();

            if (parentDir != null) {
                parentDir.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);
                updateLastModified();

                // Start watching in background
                watcherExecutor.submit(this::watchForChanges);

                logger.info("Started file watcher for configuration: {}", configFilePath);
            }

        } catch (IOException e) {
            logger.error("Failed to start file watcher for configuration", e);
        }
    }

    /**
     * Stops the file watcher.
     */
    private void stopFileWatcher() {
        if (watchService != null) {
            try {
                watchService.close();
                logger.info("Stopped file watcher for configuration");
            } catch (IOException e) {
                logger.warn("Error closing file watcher", e);
            }
            watchService = null;
        }
    }

    /**
     * Watches for file changes and reloads configuration when detected.
     */
    private void watchForChanges() {
        logger.debug("File watcher thread started");

        try {
            while (watchService != null && !Thread.currentThread().isInterrupted()) {
                WatchKey key = watchService.take();

                for (WatchEvent<?> event : key.pollEvents()) {
                    WatchEvent.Kind<?> kind = event.kind();

                    if (kind == StandardWatchEventKinds.OVERFLOW) {
                        continue;
                    }

                    @SuppressWarnings("unchecked")
                    WatchEvent<Path> pathEvent = (WatchEvent<Path>) event;
                    Path changedFile = pathEvent.context();

                    // Check if it's our config file
                    if (configFilePath.getFileName().equals(changedFile)) {
                        // Debounce rapid changes
                        if (hasFileChanged()) {
                            logger.info("Configuration file changed, reloading: {}", configFilePath);

                            // Small delay to ensure file write is complete
                            Thread.sleep(100);
                            reloadFromFile();
                        }
                    }
                }

                boolean valid = key.reset();
                if (!valid) {
                    logger.warn("File watcher key became invalid");
                    break;
                }
            }

        } catch (InterruptedException e) {
            logger.debug("File watcher thread interrupted");
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            logger.error("Error in file watcher thread", e);
        }

        logger.debug("File watcher thread stopped");
    }

    /**
     * Checks if the configuration file has actually changed since last check.
     */
    private boolean hasFileChanged() {
        try {
            Instant currentModified = Files.getLastModifiedTime(configFilePath).toInstant();
            if (currentModified.isAfter(lastModified)) {
                lastModified = currentModified;
                return true;
            }
        } catch (IOException e) {
            logger.warn("Error checking file modification time", e);
        }
        return false;
    }

    /**
     * Updates the last modified timestamp.
     */
    private void updateLastModified() {
        try {
            lastModified = Files.getLastModifiedTime(configFilePath).toInstant();
        } catch (IOException e) {
            logger.warn("Error getting file modification time", e);
            lastModified = Instant.now();
        }
    }
}
