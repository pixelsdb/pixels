//
// Created by gengdy on 24-11-17.
//

#include <executor/LoadExecutor.h>
#include <iostream>
#include <encoding/EncodingLevel.h>

void LoadExecutor::execute(const bpo::variables_map& ns, const std::string& command) {
    std::string schema = ns["schema"].as<std::string>();
    std::string origin = ns["origin"].as<std::string>();
    std::string target = ns["target"].as<std::string>();
    int rowNum = ns["row_num"].as<int>();
    std::string regex = ns["row_regex"].as<std::string>();
    int threadNum = ns["consumer_thread_num"].as<int>();
    EncodingLevel encodingLevel = EncodingLevel::from(ns["encoding_level"].as<int>());
    bool nullPadding = ns["nulls_padding"].as<bool>();

    if(origin.back() != '/') {
        origin += "/";
    }

}

bool LoadExecutor::startConsumers() {
    return true;
}