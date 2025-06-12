# Define a function to create an executable and link the required libraries
function (add_btest_executable target source)
    add_executable(btest-${target} ${source})
    target_link_libraries(btest-${target} PRIVATE ${BROKER_LIBRARY} ${ARGN})
endfunction ()

add_btest_executable(disconnect-on-overload peering/disconnect-on-overload.cc)
add_btest_executable(put-unique store/put-unique.cc CAF::core CAF::net)
add_btest_executable(sqlite-driver store/sqlite-driver.cc CAF::core CAF::net)
add_btest_executable(peers endpoint/peers.cc CAF::core CAF::net)
add_btest_executable(publish-and-receive endpoint/publish-and-receive.cc)
add_btest_executable(unpeer endpoint/unpeer.cc)
add_btest_executable(forwarding endpoint/forwarding.cc)
