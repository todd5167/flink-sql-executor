package cn.todd.flink.parser;

import org.apache.flink.sql.parser.ddl.SqlCreateFunction;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.flink.table.catalog.FunctionLanguage;

import cn.todd.flink.utils.TdStringUtils;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * flink-sql-executors项目以非交互式方式运行在yarn上，只支持create table、create view、insert、set相关语法。 Simple parser
 * for determining the type of command and its parameters.
 */
public final class SqlCommandParser {

    private static final char SQL_DELIMITER = ';';

    private SqlCommandParser() {
        // private
    }

    private static final List<ExtendedSqlParseStrategy> PARSE_STRATEGIES =
            Arrays.asList(SetParseStrategy.INSTANCE);

    public static List<SqlCommandCall> parseSqlText(String sql) {
        sql =
                TdStringUtils.dealSqlComment(sql)
                        .replaceAll("\r\n", " ")
                        .replaceAll("\n", " ")
                        .replace("\t", " ")
                        .trim();
        List<String> lines = TdStringUtils.splitIgnoreQuota(sql, SQL_DELIMITER);
        List<SqlCommandCall> calls = new ArrayList<>();

        for (String line : lines) {
            if (StringUtils.isEmpty(line)) {
                continue;
            }
            Optional<SqlCommandCall> optionalCall = parseStmt(line.toString());
            if (optionalCall.isPresent()) {
                calls.add(optionalCall.get());
            } else {
                throw new RuntimeException("Unsupported command '" + line.toString() + "'");
            }
        }
        return calls;
    }

    /** 使用 Blink parser解析出对应SqlCommandCall */
    private static Optional<SqlCommandCall> parseStmt(String stmt) {
        SqlParser.Config config = createSqlParserConfig();
        SqlParser sqlParser = SqlParser.create(stmt, config);
        SqlNodeList sqlNodes = null;
        try {
            sqlNodes = sqlParser.parseStmtList();
            // no need check the statement is valid here
        } catch (org.apache.calcite.sql.parser.SqlParseException e) {
            List<SqlCommandCall> calls =
                    PARSE_STRATEGIES.stream()
                            .map(strategy -> strategy.apply(stmt))
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList());
            if (calls.isEmpty()) {
                throw new SqlParseException(
                        String.format("Failed to parse statement: %s ", stmt), e);
            }
            return Optional.of(calls.get(0));
        }

        if (sqlNodes.size() != 1) {
            throw new SqlParseException("Only single statement is supported now");
        }

        final String[] operands;
        final SqlCommand cmd;
        SqlNode node = sqlNodes.get(0);
        if (node.getKind().belongsTo(SqlKind.QUERY)) {
            cmd = SqlCommand.SELECT;
            operands = new String[] {stmt};
        } else if (node instanceof RichSqlInsert) {
            RichSqlInsert insertNode = (RichSqlInsert) node;
            cmd = insertNode.isOverwrite() ? SqlCommand.INSERT_OVERWRITE : SqlCommand.INSERT_INTO;
            operands = new String[] {stmt, insertNode.getTargetTable().toString()};
        } else if (node instanceof SqlCreateTable) {
            cmd = SqlCommand.CREATE_TABLE;
            operands = new String[] {stmt};
        } else if (node instanceof SqlCreateView) {
            // TableEnvironment currently does not support creating view
            // so we have to perform the modification here
            SqlCreateView createViewNode = (SqlCreateView) node;
            cmd = SqlCommand.CREATE_VIEW;
            operands =
                    new String[] {
                        createViewNode.getViewName().toString(),
                        createViewNode.getQuery().toString()
                    };
        } else if (node instanceof SqlCreateFunction) {
            SqlCreateFunction createFunctionNode = (SqlCreateFunction) node;
            cmd = SqlCommand.CREATE_FUNCTION;

            String originClassName =
                    createFunctionNode.getFunctionClassName().getValue().toString();
            // 函数类转换为字符串时，会保存开始和结束的单引号，在使用时需要移除
            String functionClassName =
                    StringUtils.substring(originClassName, 1, originClassName.length() - 1);

            String[] functionIdentifier = createFunctionNode.getFunctionIdentifier();
            String functionName = functionIdentifier[functionIdentifier.length - 1];
            String functionLanguage = createFunctionNode.getFunctionLanguage();

            if (StringUtils.isEmpty(functionLanguage)) {
                functionLanguage = FunctionLanguage.JAVA.name();
            }
            operands = new String[] {stmt, functionClassName, functionName, functionLanguage};
        } else {
            cmd = null;
            operands = new String[0];
        }
        return cmd == null
                ? Optional.empty()
                : Optional.of(new SqlCommandCall(cmd, operands, node));
    }

    private static SqlParser.Config createSqlParserConfig() {
        return SqlParser.configBuilder()
                .setParserFactory(FlinkSqlParserImpl.FACTORY)
                .setConformance(FlinkSqlConformance.DEFAULT)
                .setLex(Lex.JAVA)
                .setIdentifierMaxLength(256)
                .build();
    }

    // --------------------------------------------------------------------------------------------

    /** Supported SQL commands. */
    public enum SqlCommand {
        SET,

        INSERT_INTO,

        INSERT_OVERWRITE,

        SELECT,

        CREATE_TABLE,

        CREATE_FUNCTION,

        CREATE_VIEW;

        public final Pattern pattern;
        public final Function<String[], Optional<String[]>> operandConverter;

        SqlCommand() {
            this.pattern = null;
            this.operandConverter = null;
        }

        @Override
        public String toString() {
            return super.toString().replace('_', ' ');
        }
    }

    /** Call of SQL command with operands and command type. */
    public static class SqlCommandCall {
        public final SqlCommand command;
        public final String[] operands;
        public final SqlNode sqlNode;

        public SqlCommandCall(SqlCommand command, String[] operands, SqlNode sqlNode) {
            this.command = command;
            this.operands = operands;
            this.sqlNode = sqlNode;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SqlCommandCall that = (SqlCommandCall) o;
            return command == that.command && Arrays.equals(operands, that.operands);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(command);
            result = 31 * result + Arrays.hashCode(operands);
            return result;
        }

        @Override
        public String toString() {
            return command + "(" + Arrays.toString(operands) + ")";
        }
    }
}
