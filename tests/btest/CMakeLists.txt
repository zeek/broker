# Make doctest available
include_directories(BEFORE ${CMAKE_CURRENT_SOURCE_DIR}/3rdparty)
add_executable(btest-put-unique store/put-unique.cc)

target_link_libraries(btest-put-unique PRIVATE ${BROKER_LIBRARY} CAF::core CAF::net)


add_executable(btest-sqlite-options store/sqlite-options.cc)

target_link_libraries(btest-sqlite-options PRIVATE ${BROKER_LIBRARY} CAF::core CAF::net)
