/*
 * Copyright 2024 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */

/*
 * @author gengdy
 * @create 2024-11-22
 */
#ifndef PIXELS_PIXELSCONSUMER_H
#define PIXELS_PIXELSCONSUMER_H

#include <vector>
#include <string>
#include <load/Parameters.h>

class PixelsConsumer
{
public:
    PixelsConsumer(const std::vector <std::string> &queue, const Parameters &parameters,
                   const std::vector <std::string> &loadedFiles);

    void run();

private:
    static int GlobalTargetPathId;
    std::vector <std::string> queue;
    Parameters parameters;
    std::vector <std::string> loadedFiles;
};
#endif //PIXELS_PIXELSCONSUMER_H
