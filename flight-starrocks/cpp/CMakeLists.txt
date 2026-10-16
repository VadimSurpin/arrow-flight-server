cmake_minimum_required(VERSION 3.16)
project(arrow_flight_starrocks LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Arrow REQUIRED)
find_package(ArrowFlight REQUIRED)
find_package(ArrowFlightSql REQUIRED)

add_library(arrow_flight_starrocks STATIC src/flight_scan_reader.cc)
target_include_directories(arrow_flight_starrocks PUBLIC include)
target_link_libraries(arrow_flight_starrocks
                    PUBLIC Arrow::arrow_shared
                           ArrowFlight::arrow_flight_shared
                           ArrowFlightSql::arrow_flight_sql_shared)

include(CTest)
if(BUILD_TESTING)
    add_executable(flight_scan_reader_test tests/flight_scan_reader_test.cc)
    target_link_libraries(flight_scan_reader_test PRIVATE arrow_flight_starrocks)
    add_test(NAME flight_scan_reader_test COMMAND flight_scan_reader_test)
endif()
