package cn.todd.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.CatalogFunctionImpl;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.functions.UserDefinedFunctionHelper;

import cn.todd.flink.parser.SqlCommandParser;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

/**
 * Date: 2021/10/2
 *
 * @author todd5167
 */
public class CommandOperator {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkSqlExecutor.class);

    private final TableEnvironment tableEnvironment;
    private final StreamExecutionEnvironment streamEnv;

    public CommandOperator(
            TableEnvironment tableEnvironment, StreamExecutionEnvironment streamEnv) {
        this.tableEnvironment = tableEnvironment;
        this.streamEnv = streamEnv;
    }
    /**
     * 通过执行DDL及Insert into语句，触发Flink执行。
     *
     * @param cmdCall
     * @return
     */
    public void dealDdlCommand(SqlCommandParser.SqlCommandCall cmdCall) {
        switch (cmdCall.command) {
            case CREATE_TABLE:
                callExecuteSql(cmdCall);
                break;
            case CREATE_VIEW:
                callExecuteCreateViewSql(cmdCall);
                break;
            case CREATE_FUNCTION:
                callCreateFunSql(cmdCall);
                break;
            default:
                throw new RuntimeException("Unsupported command: " + cmdCall.command);
        }
    }

    /**
     * 为TableEnvironment类所使用的类加载器,绑定udf对应的url。
     *
     * @param cmdCall
     */
    private void callCreateFunSql(SqlCommandParser.SqlCommandCall cmdCall) {
        String[] operands = cmdCall.operands;
        String sql = operands[0];
        String className = operands[1];
        String functionName = operands[2];

        boolean present =
                Arrays.stream(tableEnvironment.listFunctions())
                        .filter(
                                registeredFunName ->
                                        StringUtils.equalsIgnoreCase(
                                                registeredFunName, functionName))
                        .findAny()
                        .isPresent();
        LOG.info(String.format("register function:%s, present:%s", functionName, present));

        if (!present) {
            CatalogFunctionImpl catalogFunction = new CatalogFunctionImpl(className);
            ClassLoader classLoader = getStreamEnvUserClassLoader();
            try {
                final Class<?> functionClass =
                        classLoader.loadClass(catalogFunction.getClassName());
                UserDefinedFunction userDefinedFunction =
                        UserDefinedFunctionHelper.instantiateFunction((Class) functionClass);
                tableEnvironment.createTemporarySystemFunction(functionName, userDefinedFunction);
            } catch (Exception e) {
                throw new RuntimeException(
                        String.format(
                                "execute create function statement failed, statement is : %s", sql),
                        e);
            }
        }
    }

    private void callExecuteSql(SqlCommandParser.SqlCommandCall cmdCall) {
        String sql = cmdCall.operands[0];
        try {
            tableEnvironment.executeSql(sql);
        } catch (SqlParserException e) {
            throw new RuntimeException(String.format("execute [%] failed!", sql), e);
        }
    }

    private void callExecuteCreateViewSql(SqlCommandParser.SqlCommandCall cmdCall) {
        String viewName = cmdCall.operands[0];
        String queryStatement = cmdCall.operands[1];
        try {
            tableEnvironment.createTemporaryView(
                    viewName, tableEnvironment.sqlQuery(queryStatement));
        } catch (SqlParserException e) {
            throw new RuntimeException(
                    String.format(
                            "execute create view [%] failed, query statement is:[%s]",
                            viewName, queryStatement),
                    e);
        }
    }

    public void callMultipleInsertIntoSql(List<SqlCommandParser.SqlCommandCall> insertCalls) {
        try {
            StatementSet statementSet = tableEnvironment.createStatementSet();
            for (SqlCommandParser.SqlCommandCall cmdCall : insertCalls) {
                String insertStatement = cmdCall.operands[0];
                statementSet.addInsertSql(insertStatement);
            }
            statementSet.execute();
        } catch (SqlParserException e) {
            throw new RuntimeException("callMultipleInsertIntoSql failed!", e);
        }
    }

    public ClassLoader getStreamEnvUserClassLoader() {
        ClassLoader userClassloader = null;
        try {
            Method getUserClassloader =
                    StreamExecutionEnvironment.class.getDeclaredMethod("getUserClassloader");
            getUserClassloader.setAccessible(true);
            userClassloader = (ClassLoader) getUserClassloader.invoke(streamEnv);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return userClassloader;
    }
}
