package cn.todd.flink;

import cn.todd.flink.config.StreamEnvConfigManager;

import java.util.Properties;

/**
 * Date: 2021/10/2
 *
 * @author todd5167
 */
public class TestSqlExecutor {

    private static void testCreateView() {
        String sql =
                "CREATE TABLE user_info (\n"
                        + "    userId BIGINT,\n"
                        + "    userName VARCHAR,\n"
                        + "    proctime AS PROCTIME()\n"
                        + ") WITH (\n"
                        + "    'connector' = 'kafka', --'kafka', 'kafka-0.11', 'kafka-0.10'\n"
                        + "    'properties.bootstrap.servers' = 'localhost:9092',\n"
                        + "    'topic' = 'mqTest02', "
                        + "    'format' = 'json',\n"
                        + "    'scan.startup.mode' = 'latest-offset'\n"
                        + ");\n"
                        + "CREATE TABLE user_sink (\n"
                        + "    userId BIGINT,\n"
                        + "    userName VARCHAR,\n"
                        + "    PRIMARY KEY (userId) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "    'connector' = 'print'\n"
                        + ");\n"
                        + "create view user_info_v as\n"
                        + "  select userId, userName  from user_info;"
                        + "INSERT INTO user_sink\n"
                        + "SELECT\n"
                        + "  u.userId,\n"
                        + "  u.userName \n"
                        + "FROM\n"
                        + "  user_info  u\n";

        FlinkSqlExecutor.INSTANCE.executeSqlText(sql);
    }

    private static void testSet() {

        String sql =
                "set execution.checkpointing.interval = 3s; "
                        + "CREATE TABLE user_info (\n"
                        + "    userId BIGINT,\n"
                        + "    userName VARCHAR,\n"
                        + "    proctime AS PROCTIME()\n"
                        + ") WITH (\n"
                        + "    'connector' = 'kafka', --'kafka', 'kafka-0.11', 'kafka-0.10'\n"
                        + "    'properties.bootstrap.servers' = 'localhost:9092',\n"
                        + "    'topic' = 'mqTest02', "
                        + "    'format' = 'json',\n"
                        + "    'scan.startup.mode' = 'latest-offset'\n"
                        + ");\n"
                        + "CREATE TABLE user_sink (\n"
                        + "    userId BIGINT,\n"
                        + "    userName VARCHAR,\n"
                        + "    PRIMARY KEY (userId) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "    'connector' = 'print'\n"
                        + ");\n"
                        + "create view user_info_v as\n"
                        + "  select userId, userName  from user_info;"
                        + "INSERT INTO user_sink\n"
                        + "SELECT\n"
                        + "  u.userId,\n"
                        + "  u.userName \n"
                        + "FROM\n"
                        + "  user_info  u\n";
        FlinkSqlExecutor.INSTANCE.executeSqlText(sql);
    }

    public static void testExecute() {
        String testSql =
                "CREATE  FUNCTION str2low AS 'cn.todd.flink.udf.StrToLower' LANGUAGE SCALA;"
                        + "CREATE TABLE source_table (\n"
                        + "   id INT,\n"
                        + "   score INT,\n"
                        + "   address STRING\n"
                        + ") WITH (\n"
                        + "    'connector'='datagen',\n"
                        + "    'rows-per-second'='2',\n"
                        + "    'fields.id.kind'='sequence',\n"
                        + "    'fields.id.start'='1',\n"
                        + "    'fields.id.end'='100000',\n"
                        + "    'fields.score.min'='1',\n"
                        + "    'fields.score.max'='100',\n"
                        + "    'fields.address.length'='10'\n"
                        + ");\n"
                        + "\n"
                        + "CREATE TABLE console_table (\n"
                        + "     id INT,\n"
                        + "     score INT,\n"
                        + "     address STRING\n"
                        + ") WITH (\n"
                        + "    'connector' = 'print'\n"
                        + ");\n"
                        + "insert into console_table select id, score, str2low(address) from source_table;";

        Properties properties = new Properties();
        properties.setProperty(
                StreamEnvConfigManager.EXTERNAL_JARS, "/user/todd/code/flink-demo-1.0.jar");

        FlinkSqlExecutor.INSTANCE.executeSqlText(testSql, properties);
    }

    public static void main(String[] args) {
        testSet();
    }
}
