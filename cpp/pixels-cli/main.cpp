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
 * @create 2024-11-16
 */
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <map>
#include <algorithm>
#include <cctype>
#include <executor/LoadExecutor.h>
#include <iterator> // Add this line to include std::istream_iterator

// Trim leading and trailing whitespace from a string
std::string trim(const std::string& str)
{
  const auto first = str.find_first_not_of(' ');
  if (first == std::string::npos)
  {
    return "";
  }
  const auto last = str.find_last_not_of(' ');
  return str.substr(first, last - first + 1);
}

// Convert a string to lowercase
std::string toLower(const std::string& str)
{
  std::string lowerStr = str;
  std::transform(lowerStr.begin(), lowerStr.end(), lowerStr.begin(), [](unsigned char c) {
    return std::tolower(c);
  });
  return lowerStr;
}

// Parse command-line arguments into a map
std::map<std::string, std::string> parseArguments(const std::vector<std::string>& args)
{
  std::map<std::string, std::string> options;
  for (size_t i = 1; i < args.size(); ++i)
  {
    if (args[i].substr(0, 2) == "--")
    {
      std::string key = args[i].substr(2);
      if (i + 1 < args.size() && args[i + 1][0] != '-')
      {
        options[key] = args[i + 1];
        ++i;
      }
      else
      {
        options[key] = ""; // Flag without value
      }
    }
    else if (args[i][0] == '-')
    {
      std::string key = args[i].substr(1);
      if (i + 1 < args.size() && args[i + 1][0] != '-')
      {
        options[key] = args[i + 1];
        ++i;
      }
      else
      {
        options[key] = ""; // Flag without value
      }
    }
  }
  return options;
}

int main()
{
  std::string inputStr;

  while (true)
  {
    std::cout << "pixels> ";
    if (!std::getline(std::cin, inputStr))
    {
      // In case of input from a file, exit at EOF
      std::cout << "Bye." << std::endl;
      break;
    }
    inputStr = trim(inputStr);

    if (inputStr.empty() || inputStr == ";")
    {
      continue;
    }

    if (inputStr.back() == ';')
    {
      inputStr.pop_back();
    }

    // Process exit command
    std::string lowerInputStr = toLower(inputStr);
    if (lowerInputStr == "exit" || lowerInputStr == "quit" || lowerInputStr == "-q")
    {
      std::cout << "Bye." << std::endl;
      break;
    }

    // Process help command
    if (lowerInputStr == "help" || lowerInputStr == "-h")
    {
      std::cout << "Supported commands:\n"
                << "LOAD\n"
                << "COMPACT\n"
                << "IMPORT\n"
                << "STAT\n"
                << "QUERY\n"
                << "COPY\n"
                << "FILE_META\n";
      std::cout << "{command} -h to show the usage of a command.\nexit / quit / -q to exit.\n";
      continue;
    }

    // Split input into tokens
    std::istringstream iss(inputStr);
    std::vector<std::string> args{std::istream_iterator<std::string>{iss},
                                  std::istream_iterator<std::string>{}};
    if (args.empty())
    {
      continue;
    }

    std::string command = args[0];
    std::transform(command.begin(), command.end(), command.begin(), ::toupper);

    if (command == "LOAD")
    {
      // Parse arguments
      auto options = parseArguments(args);

      // Show help message if requested
      if (options.count("help") || options.count("h"))
      {
        std::cout << "Pixels ETL LOAD\n"
                  << "  --origin, -o <path>    Specify the path of original data files\n"
                  << "  --target, -t <path>    Specify the path of target data files\n"
                  << "  --schema, -s <schema>  Specify the schema of pixels\n"
                  << "  --row_num, -n <num>    Specify the max number of rows to write in a file\n"
                  << "  --row_regex, -r <regex> Specify the split regex of each row in a file\n"
                  << "  --encoding_level, -e <level> Specify the encoding level for data loading\n"
                  << "  --nulls_padding, -p <bool> Specify whether nulls padding is enabled\n";
        continue;
      }

      // Execute LOAD command
      LoadExecutor loadExecutor;
      loadExecutor.execute(options, command);
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
  } // End of while loop
  return 0;
}