add_executable(btest-put-unique store/put-unique.cc)
target_link_libraries(btest-put-unique PRIVATE ${BROKER_LIBRARY} CAF::core CAF::net)

add_executable(btest-sqlite-driver store/sqlite-driver.cc)
target_link_libraries(btest-sqlite-driver PRIVATE ${BROKER_LIBRARY} CAF::core CAF::net)

add_executable(btest-peers endpoint/peers.cc)
target_link_libraries(btest-peers PRIVATE ${BROKER_LIBRARY} CAF::core CAF::net)
