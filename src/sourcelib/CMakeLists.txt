set(SOURCES datetime.cpp LoadData.cpp to_string.cpp)
add_library(sourcelib STATIC ${SOURCES})
target_link_libraries(sourcelib fmt)
