cmake_minimum_required(VERSION 3.16)
# 设置C++标准为14
set(CMAKE_CXX_STANDARD 14)
project(pixels-writer-test)


set(ENV{PIXELS_SRC} "/home/whz/dev/pixels")
set(ENV{PIXELS_HOME} "/home/whz/dev/pixels")

# 查找Protobuf库，如果找不到则报错
find_package(Protobuf REQUIRED)
# 将Protobuf的包含目录添加到项目的包含目录中
include_directories(${Protobuf_INCLUDE_DIRS})

# 添加当前二进制目录到包含目录，一般用于包含自动生成的头文件等情况
include_directories(${CMAKE_CURRENT_BINARY_DIR})

# 添加pixels-common子目录，会去处理该子目录下的CMakeLists.txt文件（如果有）来构建相关内容
add_subdirectory(pixels-common)
# 同理添加pixels-core子目录
add_subdirectory(pixels-core)

# 收集所有要编译的源文件，这里可以更灵活地添加其他可能的.cpp文件，而不是重复定义
set(pixels_writer_test_cxx
        main.cpp
)

# 明确添加pixels-common和pixels-core的包含目录，这样可以确保在编译main.cpp等文件时能找到对应模块的头文件
include_directories(pixels-common/include)
include_directories(pixels-core/include)
# 添加特定的liburing相关的包含目录，同样是为了确保头文件能被找到
include_directories(${CMAKE_CURRENT_BINARY_DIR}/pixels-common/liburing/src/include)

# 创建可执行文件目标，指定名称和对应的源文件
add_executable(pixels-writer-test ${pixels_writer_test_cxx})

# 链接相关的库，这里需要确保pixels-common和pixels-core能正确生成对应的库文件并且被链接进来
# 可以添加更多的库依赖，比如Protobuf库等，如果main.cpp中使用了其功能的话
target_link_libraries(pixels-writer-test
        pixels-common
        pixels-core
        ${Protobuf_LIBRARIES}  # 添加Protobuf库的链接，前提是Protobuf库有生成对应的库文件
)

# 以下是新增部分，用于设置编译选项为Debug模式，方便调试以及让CLion更好地识别符号等
# 设置构建类型为Debug，这样会开启如-g等调试相关的编译选项
set(CMAKE_BUILD_TYPE Debug CACHE STRING "Build type" FORCE)

# 如果使用Makefile作为生成器（常用于Unix/Linux系统），添加额外的调试相关编译选项
if("${CMAKE_GENERATOR}" MATCHES "Makefile")
    set(CMAKE_C_FLAGS_DEBUG "-g -Wall")
    set(CMAKE_CXX_FLAGS_DEBUG "-g -Wall")
endif()