//
// Created by gengdy on 24-11-18.
//

#ifndef PIXELS_ENCODINGLEVEL_H
#define PIXELS_ENCODINGLEVEL_H

#include <string>

class EncodingLevel {
public:
    enum Level {
        EL0 = 0,
        EL1 = 1,
        EL2 = 2
    };

    EncodingLevel();
    explicit EncodingLevel(Level level);
    explicit EncodingLevel(int level);
    explicit EncodingLevel(const std::string &level);

    static EncodingLevel from(int level);
    static EncodingLevel from(const std::string &level);

    static bool isValid(int level);

    bool ge(int level) const;
    bool ge(const EncodingLevel &encodingLevel) const;

    bool equals(int level) const;
    bool equals(const EncodingLevel &encodingLevel)const;

private:
    Level level;
};
#endif //PIXELS_ENCODINGLEVEL_H
