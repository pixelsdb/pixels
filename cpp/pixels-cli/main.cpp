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

//
// Created by gengdy on 11/16/24.
//

#include <iostream>
#include <sstream>
#include <string>
#include <boost/algorithm/string.hpp>
#include <boost/program_options.hpp>
#include <executor/LoadExecutor.h>

namespace bpo = boost::program_options;

int main()
{
    std::string inputStr;

    while (true)
    {
        std::cout << "pixels> ";
        if (!std::getline(std::cin, inputStr))
        {
            // in case of input from a file, exit at EOF
            std::cout << "Bye." << std::endl;
            break;
        }
        boost::trim(inputStr);

        if (inputStr.empty() || inputStr == ";")
        {
            continue;
        }

        if (inputStr.back() == ';')
        {
            inputStr.pop_back();
        }

        // process exit command
        std::string lowerInputStr = boost::to_lower_copy(inputStr);
        if (lowerInputStr == "exit" || lowerInputStr == "quit" || lowerInputStr == "-q")
        {
            std::cout << "Bye." << std::endl;
            break;
        }

        // process help command
        if (lowerInputStr == "help" || lowerInputStr == "-h")
        {
            std::cout << "Supported commands:\n" <<
                      "LOAD\n" <<
                      "COMPACT\n" <<
                      "IMPORT\n" <<
                      "STAT\n" <<
                      "QUERY\n" <<
                      "COPU\n" <<
                      "FILE_META";
            std::cout << "{command} -h to show the usage of a command.\nexit / quit / -q to exit.\n";
            continue;
        }

        // Split input into tokens as char* array
        std::istringstream iss(inputStr);
        std::vector <std::string> token_strings{std::istream_iterator < std::string > {iss},
                                                std::istream_iterator < std::string > {}};
        std::vector<char *> argv;
        for (const auto &str: token_strings)
        {
            argv.push_back(strdup(str.c_str())); // Duplicate strings as char*
        }
        if (argv.empty())
        {
            continue;
        }

        std::string command = argv[0];
        boost::to_upper(command);

        if (command == "LOAD")
        {
            bpo::options_description desc("Pixels ETL LOAD");
            desc.add_options()
                    ("help,h", "show this help message and exit")
                    ("origin,o", bpo::value<std::string>()->required(), "specify the path of original data files")
                    ("target,t", bpo::value<std::string>()->required(), "specify the path of target data files")
                    ("schema,s", bpo::value<std::string>()->required(), "specify the schema of pixels")
                    ("row_num,n", bpo::value<int>()->required(), "specify the max number of rows to write in a file")
                    ("row_regex,r", bpo::value<std::string>()->required(),
                     "specify the split regex of each row in a file")
                    ("encoding_level,e", bpo::value<int>()->default_value(2),
                     "specify the encoding level for data loading")
                    ("nulls_padding,p", bpo::value<bool>()->default_value(false),
                     "specify whether nulls padding is enabled");

            bpo::variables_map vm;
            try
            {
                bpo::store(bpo::parse_command_line(argv.size(), argv.data(), desc), vm);
                if (vm.count("help"))
                {
                    std::cout << desc << std::endl;
                    continue;
                }
                bpo::notify(vm);
            }
            catch (const bpo::error &e)
            {
                std::cerr << "Error parsing options: " << e.what() << "\n";
            }
            // try {
            LoadExecutor *loadExecutor = new LoadExecutor();
            loadExecutor->execute(vm, command);
            // } catch
        }
        else if (command == "QUERY")
        {
            std::cout << "Not implemented yet." << std::endl;
        }
        else if (command == "COPY")
        {
            std::cout << "Not implemented yet." << std::endl;
        }
        else if (command == "COMPACT")
        {
            std::cout << "Not implemented yet." << std::endl;
        }
        else if (command == "STAT")
        {
            std::cout << "Not implemented yet." << std::endl;
        }
        else if (command == "IMPORT")
        {
            std::cout << "Not implemented yet." << std::endl;
        }
        else if (command == "FILE_META")
        {
            std::cout << "Not implemented yet." << std::endl;
        }
        else
        {
            std::cout << "Command " << command << " not found" << std::endl;
        }
    } // end of while loop
    return 0;
}