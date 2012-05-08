package com.treasure_data.jdbc.compiler;

/**
 * An exception class with stack trace informations
 */
public class SQLParserException extends Exception {

    /* The serial class version */
    private static final long serialVersionUID = -1099039459759769980L;

    private Throwable cause = null;

    public SQLParserException() {
        super();
    }

    public SQLParserException(String arg0) {
        super(arg0);
    }

    public SQLParserException(Throwable arg0) {
        this.cause = arg0;
    }

    public SQLParserException(String arg0, Throwable arg1) {
        super(arg0);
        this.cause = arg1;
    }

    public Throwable getCause() {
        return cause;
    }

    public void printStackTrace() {
        printStackTrace(System.err);
    }

    public void printStackTrace(java.io.PrintWriter pw) {
        super.printStackTrace(pw);
        if (cause != null) {
            pw.println("Caused by:");
            cause.printStackTrace(pw);
        }
    }

    public void printStackTrace(java.io.PrintStream ps) {
        super.printStackTrace(ps);
        if (cause != null) {
            ps.println("Caused by:");
            cause.printStackTrace(ps);
        }
    }

}
