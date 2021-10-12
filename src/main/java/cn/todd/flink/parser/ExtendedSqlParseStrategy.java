package cn.todd.flink.parser;

/**
 * Strategy to parse statement to {@link SqlCommandParser.SqlCommandCall}. parsing some special
 * command which can't supported by CalciteParser, e.g. {@code SET key=value} may contain special
 * characters in key and value.
 *
 * <p>Date: 2021/9/28
 *
 * @author todd5167
 */
public interface ExtendedSqlParseStrategy {

    /** Match and convert the input statement to the {@link SqlCommandParser.SqlCommandCall}. */
    SqlCommandParser.SqlCommandCall apply(String statement);
}
