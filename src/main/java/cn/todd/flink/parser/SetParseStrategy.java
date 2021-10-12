package cn.todd.flink.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static cn.todd.flink.parser.SqlCommandParser.SqlCommand.SET;

/**
 * 扩展set语法，set execution.checkpointing.interval = 3s
 *
 * <p>Date: 2021/9/28
 *
 * @author todd5167
 */
public class SetParseStrategy implements ExtendedSqlParseStrategy {
    protected static final int DEFAULT_PATTERN_FLAGS = Pattern.CASE_INSENSITIVE | Pattern.DOTALL;
    protected Pattern pattern;

    static final SetParseStrategy INSTANCE = new SetParseStrategy();

    protected SetParseStrategy() {
        this.pattern =
                Pattern.compile(
                        "SET(\\s+(?<key>\\S+)\\s*=\\s*('(?<quotedVal>[^']*)'|(?<val>\\S+)))?",
                        DEFAULT_PATTERN_FLAGS);
    }

    @Override
    public SqlCommandParser.SqlCommandCall apply(String statement) {
        Matcher matcher = pattern.matcher(statement.trim());
        final List<String> operands = new ArrayList<>();
        if (matcher.find()) {
            if (matcher.group("key") != null) {
                operands.add(matcher.group("key"));
                operands.add(
                        matcher.group("quotedVal") != null
                                ? matcher.group("quotedVal")
                                : matcher.group("val"));
            }
        }
        // only capture SET
        if (!operands.isEmpty() && operands.size() == 2) {
            SqlCommandParser.SqlCommandCall sqlCommandCall =
                    new SqlCommandParser.SqlCommandCall(SET, operands.toArray(new String[0]), null);
            return sqlCommandCall;
        }
        return null;
    }
}
