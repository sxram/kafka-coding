package org.sxram.kafka.tutorial;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.tuple.Pair;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Slf4j
public class Utils {

    private Utils() {
        throw new IllegalStateException("utility class");
    }

    public static Properties loadConfig(final String configFile) throws IOException {
        val cfg = new Properties();
        if (Files.exists(Paths.get(configFile))) {
            try (InputStream inputStream = new FileInputStream(configFile)) {
                cfg.load(inputStream);
            }
            return cfg;
        }

        log.info(configFile + " not found, try to load from classpath");
        try (final InputStream stream = Utils.class.getClassLoader().getResourceAsStream(configFile)) {
            if (stream == null) {
                throw new IllegalStateException("not found in classpath: " + configFile);
            }
            cfg.load(stream);
            return cfg;
        }
    }

    public static Properties mergeProperties(final String... propertyLocations) {
        return Stream.of(propertyLocations).map(loc -> {
            try {
                return Utils.loadConfig(loc);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).collect(Properties::new, Map::putAll, Map::putAll);
    }

    public static Properties mergeProperties(final Properties... properties) {
        return Stream.of(properties).collect(Properties::new, Map::putAll, Map::putAll);
    }

    public static Pair<String, String> parseKeyValue(final String message) {
        final String[] parts = message.split("-");
        final String key;
        final String value;
        if (parts.length > 1) {
            key = parts[0];
            value = parts[1];
        } else {
            key = null;
            value = parts[0];
        }
        return Pair.of(key, value);
    }

    public static void terminateApp(int durationInSeconds) {
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
        executorService.schedule(new TimerTask() {
            @Override
            public void run() {
                log.info("Terminating after " + durationInSeconds + "s");
                System.exit(0);
            }
        }, durationInSeconds, TimeUnit.SECONDS);
    }

}



