# Format-agnostic meta targets.
if (NOT TARGET doc)
  add_custom_target(doc)
endif ()

# -- HTML ---------------------------------------------------------------------

add_custom_target(broker-doc-html)

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/conf.py.in
               ${CMAKE_CURRENT_BINARY_DIR}/conf.py @ONLY)

# Copy over static files to avoid frickling with relative paths in conf.py.
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/_static
     DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/_templates
     DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

add_custom_target(broker-doc-html-init
  ${CMAKE_CURRENT_SOURCE_DIR}/scripts/initialize html
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
  COMMENT "Initializing HTML git repository" VERBATIM)

add_custom_target(broker-doc-html-build
  ${CMAKE_CURRENT_SOURCE_DIR}/scripts/build html
    ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR}
  #WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMENT "Generating HTML documentation" VERBATIM)

add_dependencies(broker-doc-html-build broker-doc-html-init)
add_dependencies(broker-doc-html broker-doc-html-build)
add_dependencies(doc broker-doc-html)

# -- PDF ----------------------------------------------------------------------

add_custom_target(broker-doc-pdf
  ${CMAKE_CURRENT_SOURCE_DIR}/scripts/build pdf
    ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMENT "Generating PDF documentation" VERBATIM)

#add_dependencies(doc broker-doc-pdf)

# -- WWW ----------------------------------------------------------------------

add_custom_target(broker-doc-deploy
  ${CMAKE_CURRENT_SOURCE_DIR}/scripts/deploy
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
  COMMENT "Deploying documentation to WWW" VERBATIM)

add_dependencies(broker-doc-deploy broker-doc-pdf)
add_dependencies(broker-doc-deploy broker-doc-html)

add_subdirectory(_examples)
