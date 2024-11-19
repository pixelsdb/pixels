//
// Created by gengdy on 24-11-17.
//

#ifndef PIXELS_LOADEXECUTOR_H
#define PIXELS_LOADEXECUTOR_H

#include <executor/CommandExecutor.h>

class LoadExecutor : public CommandExecutor {
public:
    void execute(const bpo::variables_map& ns, const std::string& command) override;
private:
    bool startConsumers();
};
#endif //PIXELS_LOADEXECUTOR_H
