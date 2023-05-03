import sys
import sqlglot
from sqlglot.errors import ErrorLevel, SqlglotError, ParseError, UnsupportedError

if __name__ == "__main__":
    sql_statement = sys.argv[1]
    from_dialect = sys.argv[2]
    to_dialect = sys.argv[3]

    # report captured exception to stderr
    try:
        result = sqlglot.transpile(sql_statement, from_dialect, to_dialect, error_level=ErrorLevel.RAISE)[0]
        print(result)
        sys.exit(0)
    except ParseError as e:
        sys.stderr.write("SQLglot parsing error: " + str(e))
        sys.stderr.flush()
        sys.exit(1)
    except UnsupportedError as e:
        sys.stderr.write("SQLglot unsupported error: " + str(e))
        sys.stderr.flush()
        sys.exit(1)
    except SqlglotError as e:
        sys.stderr.write("SQLglot general error: " + str(e))
        sys.stderr.flush()
        sys.exit(1)
