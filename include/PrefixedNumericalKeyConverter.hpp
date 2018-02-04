#pragma once

#include <cassert>
#include <cstdint>
#include <limits>
#include <type_traits>

#include <boost/endian/conversion.hpp>

#include <rocksdb/db.h>

#include "TypeHelpers.hpp"

namespace perq {
template <typename TKey, typename TPrefix>
class PrefixedNumericalKeyConverter {

  static_assert(sizeof(TKey) > sizeof(TPrefix),
                "Key size must be greater than the prefix size");
  static_assert(std::is_unsigned<TKey>() && std::is_unsigned<TPrefix>(),
                "Key and prefix types must be unsigned");

public:
  constexpr PrefixedNumericalKeyConverter(TPrefix prefix)
    : _keyTemplate(static_cast<TKey>(prefix) << (sizeof(TKey) - sizeof(TPrefix)) * 8) {}

  constexpr TKey ToKey(TKey id) const {
    id = _keyTemplate | (GetMaxId() & id);
    return boost::endian::native_to_big(id);
  }

  constexpr TKey ToId(rocksdb::Slice const& slice) const {
    assert(slice.size() == sizeof(TKey));
    return ToId(*reinterpret_cast<TKey const*>(slice.data()));
  }

  constexpr TKey ToId(TKey key) const {
    return boost::endian::big_to_native(key) & GetMaxId();
  }

  static constexpr TKey GetMaxId() {
    return static_cast<TKey>(
      ~(~typename std::conditional<sizeof(TKey) < 4, size_t, TKey>::type(0)
        << (sizeof(TKey) - sizeof(TPrefix)) * 8));
  }

private:
  TKey _keyTemplate;
};

template <typename TKey>
class PrefixedNumericalKeyConverter<TKey, NoPrefix> {

  static_assert(std::is_unsigned<TKey>(), "Key type must be unsigned");

public:
  constexpr PrefixedNumericalKeyConverter(char) {}

  constexpr TKey ToKey(TKey id) const { return boost::endian::native_to_big(id); }

  constexpr TKey ToId(rocksdb::Slice const& slice) const {
    assert(slice.size() == sizeof(TKey));
    return ToId(*reinterpret_cast<TKey const*>(slice.data()));
  }

  constexpr TKey ToId(TKey key) const { return boost::endian::big_to_native(key); }

  static constexpr TKey GetMaxId() {
    return static_cast<TKey>(
      ~typename std::conditional<sizeof(TKey) < 4, size_t, TKey>::type(0));
  }
};
}
