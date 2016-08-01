# Format-agnostic meta targets.
add_custom_target(doc)
add_custom_target(doc-deploy)

# -- HTML ---------------------------------------------------------------------

add_custom_target(doc-html)
add_dependencies(doc doc-html)

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/conf.py.in
               ${CMAKE_CURRENT_BINARY_DIR}/conf.py @ONLY)

# Copy over static files to avoid frickling with relative paths in conf.py.
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/_static
     DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/_templates
     DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

add_custom_target(doc-html-init
  ${CMAKE_CURRENT_SOURCE_DIR}/scripts/initialize html
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
  COMMENT "Initializing HTML git repository" VERBATIM)

add_custom_target(doc-html-build
  ${CMAKE_CURRENT_SOURCE_DIR}/scripts/build html
    ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR}
  #WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMENT "Generating HTML documentation" VERBATIM)

add_custom_target(doc-html-deploy
  ${CMAKE_CURRENT_SOURCE_DIR}/scripts/deploy html
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
  COMMENT "Deploying generated HTML" VERBATIM)

add_dependencies(doc-html-build doc-html-init)
add_dependencies(doc-html doc-html-build)
add_dependencies(doc-html-deploy doc-html-build)
add_dependencies(doc-deploy doc-html-deploy)

# -- PDF ----------------------------------------------------------------------

add_custom_target(doc-pdf
  ${CMAKE_CURRENT_SOURCE_DIR}/scripts/build pdf
    ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMENT "Generating PDF documentation" VERBATIM)

add_dependencies(doc doc-pdf)
