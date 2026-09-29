cmake_minimum_required(VERSION 3.22)

# pgvfs as a DuckDB extension: the C++ FileSystem adapter (extension/src)
# linked with the Rust storage crate (src/, built by cargo via Corrosion).
# Built by DuckDB's build system from extension_config.cmake (`make release`).

set(TARGET_NAME pgvfs)
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)
project(${TARGET_NAME})

include(FetchContent)
FetchContent_Declare(
  Corrosion
  GIT_REPOSITORY https://github.com/corrosion-rs/corrosion.git
  GIT_TAG v0.6.1)
FetchContent_MakeAvailable(Corrosion)

# The Rust target for the platform DuckDB is building for (extension-ci-tools
# cross-compiles macOS and Linux arm64).
if(NOT Rust_CARGO_TARGET)
  if(OS_NAME STREQUAL "osx")
    if(OSX_BUILD_ARCH STREQUAL "x86_64")
      set(Rust_CARGO_TARGET "x86_64-apple-darwin")
    else()
      set(Rust_CARGO_TARGET "aarch64-apple-darwin")
    endif()
  elseif(OS_NAME STREQUAL "linux")
    if(OS_ARCH STREQUAL "arm64" OR CMAKE_CXX_COMPILER MATCHES "aarch64")
      set(Rust_CARGO_TARGET "aarch64-unknown-linux-gnu")
    else()
      set(Rust_CARGO_TARGET "x86_64-unknown-linux-gnu")
    endif()
  elseif(WIN32 AND MSVC)
    if(OS_ARCH STREQUAL "arm64")
      set(Rust_CARGO_TARGET "aarch64-pc-windows-msvc")
    else()
      set(Rust_CARGO_TARGET "x86_64-pc-windows-msvc")
    endif()
  endif()
endif()
message(STATUS "pgvfs: Rust target ${Rust_CARGO_TARGET}")

corrosion_import_crate(
  MANIFEST_PATH ${CMAKE_CURRENT_SOURCE_DIR}/Cargo.toml
  CRATES pgvfs
  CRATE_TYPES staticlib
  PROFILE release)
# On Windows the crate's pgvfs.lib would collide with the extension's import
# library of the same name: build it into its own directory.
set_target_properties(pgvfs PROPERTIES ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/rust")

include_directories(extension/src/include)
set(EXTENSION_SOURCES extension/src/pgvfs_extension.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

foreach(target ${EXTENSION_NAME} ${LOADABLE_EXTENSION_NAME})
  target_link_libraries(${target} pgvfs-static)
  if(APPLE)
    # rustls-native-certs reads the macOS keychain.
    target_link_libraries(${target} "-framework Security" "-framework CoreFoundation")
  endif()
endforeach()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
