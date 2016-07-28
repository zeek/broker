# The meta target subsuming all formats.
add_custom_target(doc)

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/conf.py.in
               ${CMAKE_CURRENT_BINARY_DIR}/conf.py @ONLY)

# Copy over static files to avoid frickling with relative paths in conf.py.
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/_templates
     DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

# TODO: check that sphinx-build exists, and if not, suggest the user to install
# the missing dependencies via `pip install sphinx sphinx-rtd-theme`.
add_custom_target(doc-html
  sphinx-build -b html -c ${CMAKE_CURRENT_BINARY_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR}/html
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMENT "Generating HTML documentation" VERBATIM)

# Requires: pip install rst2pdf
add_custom_target(doc-pdf
  sphinx-build -b pdf -c ${CMAKE_CURRENT_BINARY_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR}/pdf
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMENT "Generating PDF documentation" VERBATIM)

add_dependencies(doc doc-html)
add_dependencies(doc doc-pdf)
