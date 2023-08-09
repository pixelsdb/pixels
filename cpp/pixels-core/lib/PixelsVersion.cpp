//
// Created by liyu on 3/13/23.
//

#include "PixelsVersion.h"
#include "exception/InvalidArgumentException.h"

int PixelsVersion::getVersion() {
    return version;
}

PixelsVersion::PixelsVersion(int v) {
    version = v;
}

PixelsVersion::Version PixelsVersion::from(int v) {
    if (v == 1) {
        return V1;
    } else {
        throw InvalidArgumentException("Wrong pixels version. ");
    }
}

bool PixelsVersion::matchVersion(PixelsVersion::Version otherVersion) {
    return otherVersion == V1;
}

PixelsVersion::Version PixelsVersion::currentVersion() {
    return V1;
}




