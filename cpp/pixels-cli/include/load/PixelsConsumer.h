//
// Created by gengdy on 24-11-22.
//

#ifndef PIXELS_PIXELSCONSUMER_H
#define PIXELS_PIXELSCONSUMER_H

#include <vector>
#include <string>
#include <load/Parameters.h>

class PixelsConsumer {
public:
    PixelsConsumer(const std::vector<std::string> &queue, const Parameters &parameters, const std::vector<std::string> &loadedFiles);
    void run();
private:
    static int GlobalTargetPathId;
    std::vector<std::string> queue;
    Parameters parameters;
    std::vector<std::string> loadedFiles;
};
#endif //PIXELS_PIXELSCONSUMER_H
