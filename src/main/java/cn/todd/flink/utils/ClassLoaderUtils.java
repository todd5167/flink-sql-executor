package cn.todd.flink.utils;

import org.apache.flink.util.function.FunctionUtils;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;

/**
 * Date: 2021/10/2
 *
 * @author todd5167
 */
public class ClassLoaderUtils {

    public static void addUrlForClassloader(String jarPaths, ClassLoader classLoader) {
        addUrlForClassloader(jarPaths, ",", classLoader);
    }

    public static void addUrlForClassloader(
            String jarPaths, String urlSeparator, ClassLoader classLoader) {
        String[] urls = jarPaths.split(urlSeparator);

        Arrays.stream(urls)
                .map(FunctionUtils.uncheckedFunction(url -> new File(url).toURI().toURL()))
                .forEach(url -> ClassLoaderUtils.addUrlForClassloader(url, classLoader));
    }

    public static void addUrlForClassloader(URL jarUrl, ClassLoader classLoader) {
        Method method = null;
        try {
            method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
        } catch (NoSuchMethodException | SecurityException e) {
            throw new RuntimeException("add url for classloader error!", e);
        }

        boolean accessible = method.isAccessible();
        try {
            if (accessible == false) {
                method.setAccessible(true);
            }
            method.invoke(classLoader, jarUrl);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            method.setAccessible(accessible);
        }
    }
}
