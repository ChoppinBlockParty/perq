#pragma once

#include <stdexcept>

#define perq_SourceLocation_CurrentLocation(x) ::perq::SourceLocation(__LINE__, x)

namespace perq {

struct SourceLocation {
  SourceLocation(std::uint_least32_t line, char const* file_name)
    : _line(line), _file_name(file_name) {}

  std::uint_least32_t line() { return _line; }

  char const* file_name() { return _file_name; }

private:
  std::uint_least32_t _line;
  char const* _file_name;
};

class Exception : public std::runtime_error {
public:
  Exception(std::string const& message, SourceLocation const& source_location)
    : std::runtime_error(message), _source_location(source_location) {}

  const SourceLocation& source_location() { return _source_location; }

private:
  SourceLocation _source_location;
};
}
