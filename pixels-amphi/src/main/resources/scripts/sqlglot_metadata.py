import sys
import json
from sqlglot import parse_one, exp
from sqlglot.errors import ErrorLevel, SqlglotError, ParseError, UnsupportedError

if __name__ == "__main__":
    query = sys.argv[1]
    field = sys.argv[2]

    # report captured exception to stderr
    try:
        if field == "column":
            column_list = []
            for column in parse_one(query).find_all(exp.Column):
                column_list.append(column.alias_or_name)

            json.dump(column_list, sys.stdout)
            sys.exit(0)
        else:
            sys.stderr.write("Unsupprted field: " + str(field))
            sys.stderr.flush()
            sys.exit(1)
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
