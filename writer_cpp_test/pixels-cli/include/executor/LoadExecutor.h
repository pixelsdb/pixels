//
// Created by gengdy on 24-11-17.
//

#ifndef PIXELS_LOADEXECUTOR_H
#define PIXELS_LOADEXECUTOR_H

#include <executor/CommandExecutor.h>
#include <vector>
#include <load/Parameters.h>

class LoadExecutor : public CommandExecutor {
public:
    void execute(const bpo::variables_map& ns, const std::string& command) override;
private:
    bool startConsumers(const std::vector<std::string> &inputFiles, Parameters parameters,
                        const std::vector<std::string> &loadedFiles);
};
#endif //PIXELS_LOADEXECUTOR_H
