//
// Created by liyu on 3/16/23.
//

#include "utils/String.h"

bool icompare_pred(unsigned char a, unsigned char b)
{
    return std::tolower(a) == std::tolower(b);
}

bool icompare(std::string const& a, std::string const& b)
{
    if (a.length()==b.length()) {
        return std::equal(b.begin(), b.end(),
                          a.begin(), icompare_pred);
    }
    else {
        return false;
    }
}
