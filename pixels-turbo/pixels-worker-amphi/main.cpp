#include <iostream>
#include "duckdb.hpp"

int main() {
    // DuckDB example
    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);
    con.Query("CREATE TABLE integers(i INTEGER)");
    con.Query("INSERT INTO integers VALUES (3)");
    auto result = con.Query("SELECT * FROM integers");
    result->Print();

    return 0;
}
