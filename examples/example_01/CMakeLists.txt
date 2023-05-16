file(GLOB_RECURSE example_files "*.hpp" "*.cpp")
add_executable(example_01 ${example_files})

target_link_libraries(example_01 PRIVATE "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Arrow::arrow_static,Arrow::arrow_shared>")


install(TARGETS example_01
        RUNTIME DESTINATION bin
        PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)