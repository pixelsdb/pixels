//
// Created by liyu on 3/13/23.
//

#ifndef PIXELS_PIXELSVERSION_H
#define PIXELS_PIXELSVERSION_H

class PixelsVersion {
public:
    enum Version {V1 = 1};
    explicit PixelsVersion(int v);
    int getVersion();
    static PixelsVersion::Version from(int v);
    static bool matchVersion(PixelsVersion::Version otherVersion);
    static PixelsVersion::Version currentVersion();
private:
    int version;
};
#endif //PIXELS_PIXELSVERSION_H
