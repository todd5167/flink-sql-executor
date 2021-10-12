package cn.todd.flink.config;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;

import cn.todd.flink.enums.EStateBackend;
import cn.todd.flink.utils.ClassLoaderUtils;
import cn.todd.flink.utils.PropertiesUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.EXTERNALIZED_CHECKPOINT;

/**
 * 为StreamExecutionEnvironment设置相关属性
 *
 * <p>Date: 2021/10/01
 *
 * @author todd5167
 */
public class StreamEnvConfigManager {
    public static final String SQL_ENV_PARALLELISM = "sql.env.parallelism";

    public static final String SQL_MAX_ENV_PARALLELISM = "sql.max.env.parallelism";

    public static final String SQL_BUFFER_TIMEOUT_MILLIS = "sql.buffer.timeout.millis";

    public static final String FLINK_TIME_CHARACTERISTIC_KEY = "time.characteristic";
    // default 200ms
    public static final String AUTO_WATERMARK_INTERVAL_KEY = "autoWatermarkInterval";

    // window early trigger
    public static final String EARLY_TRIGGER = "early.trigger";

    public static final String SQL_TTL_MINTIME = "sql.ttl.min";
    public static final String SQL_TTL_MAXTIME = "sql.ttl.max";

    public static final String STATE_BACKEND_KEY = "state.backend";
    public static final String CHECKPOINTS_DIRECTORY_KEY = "state.checkpoints.dir";
    public static final String STATE_BACKEND_INCREMENTAL_KEY = "state.backend.incremental";
    public static final String RESTOREENABLE = "restore.enable";

    // restart plocy
    public static final int FAILUEE_RATE = 3;

    public static final String FAILUREINTERVAL = "failure.interval"; // min

    public static final String DELAYINTERVAL = "delay.interval"; // sec

    /** LocalStreamEnvironment 下用户程序依赖的外部jar包，方便本地调试。例如：udf jar包 */
    public static final String EXTERNAL_JARS = "external.jars";

    /**
     * 配置StreamExecutionEnvironment运行时参数
     *
     * @param streamEnv
     * @param confProperties
     */
    public static void streamExecutionEnvironmentConfig(
            StreamExecutionEnvironment streamEnv, Properties confProperties)
            throws InvocationTargetException, IllegalAccessException, NoSuchMethodException,
                    IOException {

        confProperties = PropertiesUtils.propertiesTrim(confProperties);
        streamEnv.getConfig().disableClosureCleaner();

        Configuration globalJobParameters = new Configuration();
        // Configuration unsupported set properties key-value
        Method method =
                Configuration.class.getDeclaredMethod(
                        "setValueInternal", String.class, Object.class);
        method.setAccessible(true);
        for (Map.Entry<Object, Object> prop : confProperties.entrySet()) {
            method.invoke(globalJobParameters, prop.getKey(), prop.getValue());
        }

        ExecutionConfig exeConfig = streamEnv.getConfig();
        if (exeConfig.getGlobalJobParameters() == null) {
            exeConfig.setGlobalJobParameters(globalJobParameters);
        } else if (exeConfig.getGlobalJobParameters() != null) {
            exeConfig.setGlobalJobParameters(globalJobParameters);
        }

        disableChainOperator(streamEnv, globalJobParameters);

        getEnvParallelism(confProperties).ifPresent(streamEnv::setParallelism);
        getMaxEnvParallelism(confProperties).ifPresent(streamEnv::setMaxParallelism);
        getBufferTimeoutMillis(confProperties).ifPresent(streamEnv::setBufferTimeout);
        getStreamTimeCharacteristic(confProperties)
                .ifPresent(streamEnv::setStreamTimeCharacteristic);
        getAutoWatermarkInterval(confProperties)
                .ifPresent(
                        op -> {
                            if (streamEnv
                                    .getStreamTimeCharacteristic()
                                    .equals(TimeCharacteristic.EventTime)) {
                                streamEnv.getConfig().setAutoWatermarkInterval(op);
                            }
                        });

        if (isRestore(confProperties).get()) {
            streamEnv.setRestartStrategy(
                    RestartStrategies.failureRateRestart(
                            FAILUEE_RATE,
                            Time.of(getFailureInterval(confProperties).get(), TimeUnit.MINUTES),
                            Time.of(getDelayInterval(confProperties).get(), TimeUnit.SECONDS)));
        } else {
            streamEnv.setRestartStrategy(RestartStrategies.noRestart());
        }

        Optional<Boolean> checkpointingEnabled = isCheckpointingEnabled(confProperties);
        if (checkpointingEnabled.get()) {
            getCheckpointCleanup(confProperties)
                    .ifPresent(streamEnv.getCheckpointConfig()::enableExternalizedCheckpoints);
            getStateBackend(confProperties).ifPresent(streamEnv::setStateBackend);
        }
        // use flink internal checkpoint parameters
        streamEnv.getCheckpointConfig().configure(globalJobParameters);
    }

    /**
     * 设置TableEnvironment window提前触发
     *
     * @param tableEnv
     * @param confProperties
     */
    public static void streamTableEnvironmentEarlyTriggerConfig(
            TableEnvironment tableEnv, Properties confProperties) {
        confProperties = PropertiesUtils.propertiesTrim(confProperties);
        String triggerTime = confProperties.getProperty(EARLY_TRIGGER);
        if (StringUtils.isNumeric(triggerTime)) {
            TableConfig qConfig = tableEnv.getConfig();
            qConfig.getConfiguration().setString("table.exec.emit.early-fire.enabled", "true");
            qConfig.getConfiguration()
                    .setString("table.exec.emit.early-fire.delay", triggerTime + "s");
        }
    }

    /**
     * 设置TableEnvironment状态超时时间
     *
     * @param tableEnv
     * @param confProperties
     */
    public static void streamTableEnvironmentStateTTLConfig(
            TableEnvironment tableEnv, Properties confProperties) {
        confProperties = PropertiesUtils.propertiesTrim(confProperties);
        Optional<Tuple2<Time, Time>> tableEnvTTL = getTableEnvTTL(confProperties);
        if (tableEnvTTL.isPresent()) {
            Tuple2<Time, Time> timeRange = tableEnvTTL.get();
            TableConfig qConfig = tableEnv.getConfig();
            qConfig.setIdleStateRetentionTime(timeRange.f0, timeRange.f1);
        }
    }

    /** 为StreamTableEnvironment设置外部jar url */
    public static void streamTableEnvironmentClassLoaderConfig(
            StreamExecutionEnvironment streamEnv, Properties confProperties) {
        String externalJars = confProperties.getProperty(EXTERNAL_JARS);
        // add user jars url to classloader
        Optional.ofNullable(externalJars)
                .ifPresent(
                        (userJars) -> {
                            ClassLoader classLoader = streamEnv.getClass().getClassLoader();
                            ClassLoaderUtils.addUrlForClassloader(userJars, classLoader);
                        });
    }

    // -----------------------StreamExecutionEnvironment
    public static Optional<Integer> getEnvParallelism(Properties properties) {
        String parallelismStr = properties.getProperty(SQL_ENV_PARALLELISM);
        return StringUtils.isNotBlank(parallelismStr)
                ? Optional.of(Integer.valueOf(parallelismStr))
                : Optional.empty();
    }

    public static Optional<Integer> getMaxEnvParallelism(Properties properties) {
        String parallelismStr = properties.getProperty(SQL_MAX_ENV_PARALLELISM);
        return StringUtils.isNotBlank(parallelismStr)
                ? Optional.of(Integer.valueOf(parallelismStr))
                : Optional.empty();
    }

    public static Optional<Long> getBufferTimeoutMillis(Properties properties) {
        String mills = properties.getProperty(SQL_BUFFER_TIMEOUT_MILLIS);
        return StringUtils.isNotBlank(mills) ? Optional.of(Long.valueOf(mills)) : Optional.empty();
    }

    public static Optional<Long> getAutoWatermarkInterval(Properties properties) {
        String autoWatermarkInterval = properties.getProperty(AUTO_WATERMARK_INTERVAL_KEY);
        return StringUtils.isNotBlank(autoWatermarkInterval)
                ? Optional.of(Long.valueOf(autoWatermarkInterval))
                : Optional.empty();
    }

    public static Optional<Boolean> isRestore(Properties properties) {
        String restoreEnable = properties.getProperty(RESTOREENABLE, "true");
        return Optional.of(Boolean.valueOf(restoreEnable));
    }

    public static Optional<Integer> getDelayInterval(Properties properties) {
        String delayInterval = properties.getProperty(DELAYINTERVAL, "10");
        return Optional.of(Integer.valueOf(delayInterval));
    }

    public static Optional<Integer> getFailureInterval(Properties properties) {
        String failureInterval = properties.getProperty(FAILUREINTERVAL, "6");
        return Optional.of(Integer.valueOf(failureInterval));
    }

    /**
     * #ProcessingTime(默认), IngestionTime, EventTime
     *
     * @param properties
     */
    public static Optional<TimeCharacteristic> getStreamTimeCharacteristic(Properties properties) {
        if (!properties.containsKey(FLINK_TIME_CHARACTERISTIC_KEY)) {
            return Optional.empty();
        }
        String characteristicStr = properties.getProperty(FLINK_TIME_CHARACTERISTIC_KEY);
        Optional<TimeCharacteristic> characteristic =
                Arrays.stream(TimeCharacteristic.values())
                        .filter(tc -> characteristicStr.equalsIgnoreCase(tc.toString()))
                        .findAny();

        if (!characteristic.isPresent()) {
            throw new RuntimeException("illegal property :" + FLINK_TIME_CHARACTERISTIC_KEY);
        }
        return characteristic;
    }

    public static Optional<Boolean> isCheckpointingEnabled(Properties properties) {
        boolean checkpointEnabled =
                properties.getProperty(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL.key())
                        != null;
        return Optional.of(checkpointEnabled);
    }

    public static Optional<CheckpointConfig.ExternalizedCheckpointCleanup> getCheckpointCleanup(
            Properties properties) {
        Boolean ckCleanMode =
                BooleanUtils.toBoolean(
                        properties.getProperty(EXTERNALIZED_CHECKPOINT.key(), "false"));

        CheckpointConfig.ExternalizedCheckpointCleanup externalizedCheckpointCleanup =
                (ckCleanMode)
                        ? CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION
                        : CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;
        return Optional.of(externalizedCheckpointCleanup);
    }

    public static Optional<StateBackend> getStateBackend(Properties properties) throws IOException {
        String backendType = properties.getProperty(STATE_BACKEND_KEY);
        String checkpointDataUri = properties.getProperty(CHECKPOINTS_DIRECTORY_KEY);
        String backendIncremental = properties.getProperty(STATE_BACKEND_INCREMENTAL_KEY, "true");

        if (!StringUtils.isEmpty(backendType)) {
            return createStateBackend(backendType, checkpointDataUri, backendIncremental);
        }
        return Optional.empty();
    }

    private static Optional<StateBackend> createStateBackend(
            String backendType, String checkpointDataUri, String backendIncremental)
            throws IOException {
        EStateBackend stateBackendType = EStateBackend.convertFromString(backendType);
        StateBackend stateBackend = null;
        switch (stateBackendType) {
            case MEMORY:
                stateBackend = new MemoryStateBackend();
                break;
            case FILESYSTEM:
                checkpointDataUriEmptyCheck(checkpointDataUri, backendType);
                stateBackend = new FsStateBackend(checkpointDataUri);
                break;
            case ROCKSDB:
                checkpointDataUriEmptyCheck(checkpointDataUri, backendType);
                stateBackend =
                        new RocksDBStateBackend(
                                checkpointDataUri, BooleanUtils.toBoolean(backendIncremental));
                break;
            default:
                break;
        }
        return stateBackend == null ? Optional.empty() : Optional.of(stateBackend);
    }

    private static void checkpointDataUriEmptyCheck(String checkpointDataUri, String backendType) {
        if (StringUtils.isEmpty(checkpointDataUri)) {
            throw new RuntimeException(backendType + " backend checkpointDataUri not null!");
        }
    }

    // -----------------TableEnvironment state ttl config------------------------------

    private static final String TTL_PATTERN_STR = "^+?([1-9][0-9]*)([dDhHmMsS])$";
    private static final Pattern TTL_PATTERN = Pattern.compile(TTL_PATTERN_STR);

    public static Optional<Tuple2<Time, Time>> getTableEnvTTL(Properties properties) {
        String ttlMintimeStr = properties.getProperty(SQL_TTL_MINTIME);
        String ttlMaxtimeStr = properties.getProperty(SQL_TTL_MAXTIME);
        if (StringUtils.isNotEmpty(ttlMintimeStr) || StringUtils.isNotEmpty(ttlMaxtimeStr)) {
            verityTtl(ttlMintimeStr, ttlMaxtimeStr);
            Matcher ttlMintimeStrMatcher = TTL_PATTERN.matcher(ttlMintimeStr);
            Matcher ttlMaxtimeStrMatcher = TTL_PATTERN.matcher(ttlMaxtimeStr);

            Long ttlMintime = 0L;
            Long ttlMaxtime = 0L;
            if (ttlMintimeStrMatcher.find()) {
                ttlMintime =
                        getTtlTime(
                                Integer.parseInt(ttlMintimeStrMatcher.group(1)),
                                ttlMintimeStrMatcher.group(2));
            }
            if (ttlMaxtimeStrMatcher.find()) {
                ttlMaxtime =
                        getTtlTime(
                                Integer.parseInt(ttlMaxtimeStrMatcher.group(1)),
                                ttlMaxtimeStrMatcher.group(2));
            }
            if (0L != ttlMintime && 0L != ttlMaxtime) {
                return Optional.of(
                        new Tuple2<>(Time.milliseconds(ttlMintime), Time.milliseconds(ttlMaxtime)));
            }
        }
        return Optional.empty();
    }

    /**
     * ttl 校验
     *
     * @param ttlMintimeStr 最小时间
     * @param ttlMaxtimeStr 最大时间
     */
    private static void verityTtl(String ttlMintimeStr, String ttlMaxtimeStr) {
        if (null == ttlMintimeStr
                || null == ttlMaxtimeStr
                || !TTL_PATTERN.matcher(ttlMintimeStr).find()
                || !TTL_PATTERN.matcher(ttlMaxtimeStr).find()) {
            throw new RuntimeException(
                    "sql.ttl.min 、sql.ttl.max must be set at the same time . example sql.ttl.min=1h,sql.ttl.max=2h");
        }
    }

    /**
     * 不同单位时间到毫秒的转换
     *
     * @param timeNumber 时间值，如：30
     * @param timeUnit 单位，d:天，h:小时，m:分，s:秒
     * @return
     */
    private static Long getTtlTime(Integer timeNumber, String timeUnit) {
        if ("d".equalsIgnoreCase(timeUnit)) {
            return timeNumber * 1000L * 60 * 60 * 24;
        } else if ("h".equalsIgnoreCase(timeUnit)) {
            return timeNumber * 1000L * 60 * 60;
        } else if ("m".equalsIgnoreCase(timeUnit)) {
            return timeNumber * 1000L * 60;
        } else if ("s".equalsIgnoreCase(timeUnit)) {
            return timeNumber * 1000L;
        } else {
            throw new RuntimeException("not support " + timeNumber + timeUnit);
        }
    }

    private static void disableChainOperator(
            StreamExecutionEnvironment env, Configuration configuration) {
        if (configuration.getBoolean("disableChainOperator", false)) {
            env.disableOperatorChaining();
        }
    }
}
