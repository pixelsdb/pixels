//
// Created by gengdy on 24-11-17.
//

#ifndef PIXELS_COMMANDEXECUTOR_H
#define PIXELS_COMMANDEXECUTOR_H

#include <boost/program_options.hpp>
#include <string>

namespace bpo = boost::program_options;

class CommandExecutor {
public:
    virtual ~CommandExecutor() = default;
    virtual void execute(const bpo::variables_map& ns, const std::string& command) = 0;
};
#endif //PIXELS_COMMANDEXECUTOR_H
