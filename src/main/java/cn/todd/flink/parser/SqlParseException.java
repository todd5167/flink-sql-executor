package cn.todd.flink.parser;

/** Exception thrown during parsing SQL statement. */
public class SqlParseException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public SqlParseException(String message) {
        super(message);
    }

    public SqlParseException(String message, Throwable e) {
        super(message, e);
    }
}
