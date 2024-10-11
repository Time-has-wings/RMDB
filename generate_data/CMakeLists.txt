cmake_minimum_required(VERSION 3.16)

set(CMAKE_CXX_STANDARD 20)
add_executable(make_sql generate_data_file.cpp clock.cpp random.cpp)
