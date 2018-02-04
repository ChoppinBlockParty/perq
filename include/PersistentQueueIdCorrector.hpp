#pragma once

#include <limits>
#include <type_traits>

#include "Exception.hpp"

/*
 * Facilitates correction on crash recovery of Persistent Queue by validating and
 * suggesting that the sequence of IDs is consecutive numbers.
 *
 * For more details see comments in `PersistentQueue.hpp`
 *
 */

#define CurrentLocation                                                                  \
  perq_SourceLocation_CurrentLocation("PersistentQueueIdCorrector.hpp")

namespace perq {
template <typename T>
class PersistentQueueIdCorrector {
  static_assert(std::is_unsigned<T>(), "Must be unsigned integer type");

public:
  /*
   *  `head`     - starting ID.
   * 	`max`      - maximum ID value
	 *  `max_diff` - maximum difference between consequent IDs that considered to a
	 *               recoverable crash gap. If differences between consequent IDs is greater
	 *               than this number, it is considered to be the end of a queue (a natural
	 *               difference from tail to head).
	 */
  PersistentQueueIdCorrector(T head, T max, size_t max_diff)
    : _max(max), _max_diff(max_diff), _is_over_end(), _head(head), _tail(head),
      _previous_checked_head(std::numeric_limits<T>::max()),
      _previous_checked_tail(std::numeric_limits<T>::max()) {
    if (_head > _max) {
      throw Exception(
        "PersistentQueueIdCorrector initialization failure: provided head ID is greater than the maximum ID",
        CurrentLocation);
    }

    if (_max_diff == 0 || _max_diff >= _max) {
      throw Exception(
        "PersistentQueueIdCorrector initialization failure: provided maximum difference is zero, or greater or equal to the maximum ID",
        CurrentLocation);
    }
  }

  T head() const { return _head; }

  T tail() const { return _tail; }

  T previous_checked_head() const { return _previous_checked_head; }

  T previous_checked_tail() const { return _previous_checked_tail; }

  void SetTailToPrevious() {
    if (!IsOverEnd() || !IsTailMax())
      throw Exception("Severe misuse of `PersistentQueueIdCorrector::SetTailToPrevious`",
                      CurrentLocation);
    _tail = _previous_checked_tail;
  }

  bool IsOverEnd() const { return _is_over_end; }

  T IsTailMax() const { return _tail == _max; }

  T FeedNext(T id) {
    if (id > _max)
      throw Exception(
        "Severe misuse of `FeedNext`: the provided ID is greater than the maximum ID",
        CurrentLocation);

    if (_tail == _max) {
      if (!_is_over_end)
        throw Exception(
          "Severe misuse of `PersistentQueueIdCorrector::FeedNext`: the queue is not over the end, but next ID passes the end",
          CurrentLocation);
      _tail = 0;
      return 0;
    }

    if (id <= _tail)
      throw Exception(
        "Severe misuse of `PersistentQueueIdCorrector::FeedNext`: the provided ID is less or equal than the tail",
        CurrentLocation);

    if (static_cast<size_t>(id - _tail) <= _max_diff) {
      ++_tail;
      return _tail;
    }

    // The queue's head is greater than the tail.
    // This means that queue at some point reached the maximum ID, and its tail was reset
    // to 0. At this point we need to reset the head and the tail to the next ID and
    // proceed correction from there.
    if (_is_over_end)
      throw Exception(
        "Severe misuse of `PersistentQueueIdCorrector::FeedNext`: the queue is over then end for the second time",
        CurrentLocation);
    _is_over_end = true;
    _previous_checked_head = _head;
    _previous_checked_tail = _tail;
    _head = _tail = id;
    return id;
  }

private:
  T _max;
  size_t _max_diff;
  bool _is_over_end;
  T _head;
  T _tail;
  T _previous_checked_head;
  T _previous_checked_tail;
};
}

#undef CurrentLocation
