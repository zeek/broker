add_executable(btest-put-unique store/put-unique.cpp)

target_link_libraries(btest-put-unique PRIVATE ${BROKER_LIBRARY} CAF::core CAF::net)
