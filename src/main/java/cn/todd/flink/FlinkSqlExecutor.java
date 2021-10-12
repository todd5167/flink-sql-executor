package cn.todd.flink;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.function.FunctionUtils;

import cn.todd.flink.config.StreamEnvConfigManager;
import cn.todd.flink.parser.SqlCommandParser;
import cn.todd.flink.utils.JsonUtils;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static cn.todd.flink.parser.SqlCommandParser.SqlCommand.SET;

/**
 * 支持原生flink sql语句的执行，可以作为独立的jar任务执行。
 *
 * @author todd5167
 */
public enum FlinkSqlExecutor {
    INSTANCE;

    private static final Logger LOG = LoggerFactory.getLogger(FlinkSqlExecutor.class);

    private static final String SQL_FILE_PATH = "sqlFilePath";
    private static final String FLINK_PROPERTIES = "flinkProperties";

    private TableEnvironment tableEnvironment;
    private StreamExecutionEnvironment streamEnv;

    public void executeSqlFile(String sqlFilePath) throws IOException {
        sqlFilePathValidate(sqlFilePath);

        String sqlText = new String(FileUtils.readAllBytes(Paths.get(sqlFilePath)));
        this.executeSqlText(sqlText);
    }

    public void executeSqlFile(String sqlFilePath, Properties confProperties) throws IOException {
        sqlFilePathValidate(sqlFilePath);

        String sqlText = new String(FileUtils.readAllBytes(Paths.get(sqlFilePath)));
        this.executeSqlText(sqlText, confProperties);
    }

    public void executeSqlText(String sqlText) {
        this.executeSqlText(sqlText, new Properties());
    }

    /**
     * 包含Insert into的Flink sql执行。
     *
     * @param sqlText
     * @param confProperties
     * @throws Exception
     */
    public void executeSqlText(String sqlText, Properties confProperties) {
        String conf = confProperties == null ? "" : confProperties.toString();
        LOG.info("execute sql statements: {}, confProperties:{}", sqlText, conf);

        List<SqlCommandParser.SqlCommandCall> calls = SqlCommandParser.parseSqlText(sqlText);
        // process Set operation, filter out set instructions and fill in confProperties
        calls.stream()
                .filter(call -> call.command == SET)
                .forEach(
                        setOperand ->
                                confProperties.put(setOperand.operands[0], setOperand.operands[1]));
        tableEnvironment = getStreamTableEnv(confProperties);

        CommandOperator commandOperator = new CommandOperator(tableEnvironment, streamEnv);
        List<SqlCommandParser.SqlCommandCall> insertCalls = Lists.newArrayList();
        for (SqlCommandParser.SqlCommandCall cmdCall : calls) {
            switch (cmdCall.command) {
                case INSERT_INTO:
                case INSERT_OVERWRITE:
                    insertCalls.add(cmdCall);
                    break;
                case SET:
                    break;
                default:
                    commandOperator.dealDdlCommand(cmdCall);
            }
        }
        commandOperator.callMultipleInsertIntoSql(insertCalls);
    }

    public StreamTableEnvironment getStreamTableEnv(Properties confProperties) {
        // build stream exec env and fill properties
        streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        Optional.ofNullable(confProperties)
                .ifPresent(
                        FunctionUtils.uncheckedConsumer(
                                (conf) ->
                                        StreamEnvConfigManager.streamExecutionEnvironmentConfig(
                                                streamEnv, conf)));
        // use blink and stream mode
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        // build tableEnv and fill properties
        StreamTableEnvironment tableEnv =
                StreamTableEnvironmentImpl.create(streamEnv, settings, new TableConfig());

        Optional.ofNullable(confProperties)
                .ifPresent(
                        (conf) ->
                                StreamEnvConfigManager.streamTableEnvironmentStateTTLConfig(
                                        tableEnv, conf));
        Optional.ofNullable(confProperties)
                .ifPresent(
                        (conf) ->
                                StreamEnvConfigManager.streamTableEnvironmentEarlyTriggerConfig(
                                        tableEnv, conf));

        if (streamEnv instanceof LocalStreamEnvironment) {
            Optional.ofNullable(confProperties)
                    .ifPresent(
                            (conf) ->
                                    StreamEnvConfigManager.streamTableEnvironmentClassLoaderConfig(
                                            streamEnv, conf));
        }
        return tableEnv;
    }

    public static void sqlFilePathValidate(String sqlFilePath) {
        if (StringUtils.isEmpty(sqlFilePath)) {
            throw new IllegalArgumentException(String.valueOf("sql file path is not null!"));
        }

        File sqlFile = new File(sqlFilePath);
        if (!(sqlFile.exists() && sqlFile.isFile())) {
            throw new IllegalArgumentException(
                    String.valueOf("sql file is not exits ,or file path is not a file !"));
        }
    }

    public static void main(String[] args) throws Exception {
        LOG.info("------------program params-------------------------");
        Arrays.stream(args).forEach(arg -> LOG.info("args: {}", arg));
        LOG.info("-------------------------------------------");

        final ParameterTool params = ParameterTool.fromArgs(args);
        String sqlFilePath = params.get(SQL_FILE_PATH);

        String flinkProperties = params.get(FLINK_PROPERTIES);

        if (StringUtils.isEmpty(flinkProperties)) {
            FlinkSqlExecutor.INSTANCE.executeSqlFile(sqlFilePath);
        } else {
            Properties properties = JsonUtils.jsonStrToObject(flinkProperties, Properties.class);
            FlinkSqlExecutor.INSTANCE.executeSqlFile(sqlFilePath, properties);
        }
    }
}
