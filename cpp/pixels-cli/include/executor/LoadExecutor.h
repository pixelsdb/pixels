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
 * @create 2024-11-17
 */
#ifndef PIXELS_LOADEXECUTOR_H
#define PIXELS_LOADEXECUTOR_H

#include <executor/CommandExecutor.h>
#include <vector>
#include <load/Parameters.h>

class LoadExecutor : public CommandExecutor
{
public:
    void execute(const bpo::variables_map &ns, const std::string &command) override;

private:
    bool startConsumers(const std::vector <std::string> &inputFiles, Parameters parameters,
                        const std::vector <std::string> &loadedFiles);
};
#endif //PIXELS_LOADEXECUTOR_H
