#pragma once

#include <cinttypes>
#include <cstddef>

namespace perq {

struct NoPrefix {
  using Type = uint8_t;
};

namespace internal {

template <typename T>
struct PrefixSize {
  static constexpr size_t size = sizeof(T);
};

template <>
struct PrefixSize<NoPrefix> {
  static constexpr size_t size = 0;
};
}
}
