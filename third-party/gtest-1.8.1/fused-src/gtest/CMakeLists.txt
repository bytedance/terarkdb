project(gtest)

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-sign-compare")
SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-sign-compare")

INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/../)

ADD_LIBRARY(gtest gtest-all.cc)

# Add dependency of gtest on thread library
target_link_libraries(gtest ${CMAKE_THREAD_LIBS_INIT})

#INSTALL(TARGETS gtest
#        EXPORT gtest-targets
#        ARCHIVE DESTINATION ${CMAKE_BINARY_DIR}/lib
#        )

INSTALL(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/../
        DESTINATION ${CMAKE_BINARY_DIR}/include
        FILES_MATCHING 
        PATTERN "*.h"
        PATTERN "*.hpp"
)
