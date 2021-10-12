package cn.todd.flink.utils;

import java.util.Properties;

public class PropertiesUtils {
    /**
     * property k v trim
     *
     * @param confProperties
     * @return
     */
    public static Properties propertiesTrim(Properties confProperties) {
        Properties properties = new Properties();
        confProperties.forEach(
                (k, v) -> {
                    properties.put(k.toString().trim(), v.toString().trim());
                });
        return properties;
    }
}
