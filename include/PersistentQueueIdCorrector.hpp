#pragma once

#include <limits>
#include <type_traits>

#include "Exception.hpp"

/*
 * Facilitates correction on crash recovery of Persistent Queue by validating and
 * suggesting that the sequence of IDs is consecutive numbers.
 *
 * Persistent Queue uses consecutive numbers as IDs for persistency. * That means that
 * every next number inserted element in the queue receives `tail + 1`
 * ID.
 *
 * 1. When insertion is done concurrently, parallel inserts will be executed in any order.
 * In case of crash during concurrent insetion consecutive numbering might be broken, when
 * one insert succeeded and a previous insert did not (e.g. 1-success, 2-success, <crash>,
 * 3-fail, 5-success). This case must be recoved on startup by moving next rows to
 * missing IDs.
 *
 * 2. Queue IDs may reach a maximum available ID. In this case tail ID must be reset to 0
 * for the next insertion. Since on startup it is not known where is tail and where is
 * head. This case must be detected. This happens when consecutive IDs are broken and
 * there is a large gap between them. This gap must not be distinguished from the gap
 * described in (1). This is done by taking into account the fact that the crash gap from
 * (1) is of maximum size of possible parallel insertions what in generall case is a
 * maximum number of threads.
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
   *  `head`  - starting ID.
   * 	`max`   - maximum ID value
	 *  `max_diff` - maximum difference between consequent IDs that considered to a
	 *               recoverable crash gap. If differences between consequent IDs is greater
	 *               than this number, it is considered to be the end of a queue (a natural
	 *               difference from tail to head).
	 */
  PersistentQueueIdCorrector(T head, T max, T max_diff)
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

    if (id - _tail <= _max_diff) {
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
  T _max_diff;
  bool _is_over_end;
  T _head;
  T _tail;
  T _previous_checked_head;
  T _previous_checked_tail;
};
}

#undef CurrentLocation
