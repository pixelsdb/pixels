import sys
import sqlglot

if __name__ == "__main__":
    sql_statement = sys.argv[1]
    from_dialect = sys.argv[2]
    to_dialect = sys.argv[3]

    result = sqlglot.transpile(sql_statement, from_dialect, to_dialect)[0]
    print(result)