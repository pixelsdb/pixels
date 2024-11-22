//
// Created by gengdy on 24-11-22.
//

#include "load/PixelsConsumer.h"
#include <iostream>

int PixelsConsumer::GlobalTargetPathId = 0;

PixelsConsumer::PixelsConsumer(const std::vector <std::string> &queue, const Parameters &parameters,
                               const std::vector <std::string> &loadedFiles)
                               : queue(queue), parameters(parameters), loadedFiles(loadedFiles) {}

void PixelsConsumer::run() {
    std::cout << "Start PixelsConsumer" << std::endl;
}