add_executable(btest-put-unique store/put-unique.cc)

target_link_libraries(btest-put-unique PRIVATE ${BROKER_LIBRARY} CAF::core CAF::net)
