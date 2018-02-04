#pragma once

#include <atomic>
#include <cassert>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>

#include <rocksdb/db.h>

#include "Exception.hpp"
#include "PersistentQueueIdCorrector.hpp"
#include "PrefixedNumericalKeyConverter.hpp"
#include "Stats.hpp"
#include "TypeHelpers.hpp"

/*
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
 * (1) is of maximum size of possible parallel insertions what pessimistically case is the
 * maximum number of threads. On Unix system see `/proc/sys/kernel/threads-max`.
 *
 */

#define CurrentLocation perq_SourceLocation_CurrentLocation("PersistentQueue.hpp")

namespace perq {
template <typename TKey,
          typename TPrefix = NoPrefix,
          typename std::conditional<std::is_same<TPrefix, NoPrefix>::value,
                                    typename NoPrefix::Type,
                                    TPrefix>::type prefixValue
          = 0>
class PersistentQueue {

  static_assert(sizeof(TKey) > internal::PrefixSize<TPrefix>::size,
                "Key size must be greater than the prefix size");
  static_assert(std::is_unsigned<TKey>()
                  && (std::is_unsigned<TPrefix>() || std::is_same<TPrefix, NoPrefix>()),
                "Key and prefix types must be unsigned");

public:
  PersistentQueue() : _db(), _max_thread_number(std::numeric_limits<size_t>::max()) {}

  PersistentQueue(rocksdb::DB* db, size_t max_thread_number = default_max_thread_number)
    : PersistentQueue() {
    Initialize(db, max_thread_number);
  }

  void Initialize(rocksdb::DB* db, size_t max_thread_number = default_max_thread_number) {
    if (_db)
      throw Exception(
        "Fatal error: attempt to initialize PersistentQueue for a second time",
        CurrentLocation);

    if (max_thread_number >= _conv.GetMaxId())
      throw Exception("Maximum number of threads (" + std::to_string(max_thread_number)
                        + ") is too large, no item would be able to exist in the queue",
                      CurrentLocation);

    _db = db;
    _max_thread_number = max_thread_number;

    auto it
      = std::unique_ptr<rocksdb::Iterator>(_db->NewIterator(rocksdb::ReadOptions()));

    TKey key = _conv.ToKey(0);
    rocksdb::Slice slice = ToSlice(&key);

    it->Seek(slice);

    if (!it->Valid()) {
      if (!it->status().ok())
        throw Exception("Fatal error in RocksDB at `Iterator::Seek`: "
                          + it->status().ToString(),
                        CurrentLocation);

      // Queue is empty, fine.
      _head.store(0, std::memory_order_relaxed);
      _next_tail.store(0, std::memory_order_relaxed);
      return;
    }

    // Queue is not empty, we need to find the head and the tail

    auto corrector = PersistentQueueIdCorrector<TKey>(
      _conv.ToId(it->key()), _conv.GetMaxId(), _max_thread_number);

    for (it->Next();; it->Next()) {
      if (!it->Valid()) {
        if (!it->status().ok())
          throw Exception("Fatal error in RocksDB at `Iterator::Next`: "
                            + it->status().ToString(),
                          CurrentLocation);

        if (!corrector.IsOverEnd())
          break;

        Seek(it, 0);
      }

      if (it->key().size() != sizeof(TKey))
        throw Exception("Fatal queue data state: a found key size ("
                          + std::to_string(it->key().size())
                          + ") != the current key size ("
                          + std::to_string(sizeof(TKey))
                          + ")",
                        CurrentLocation);

      const auto id = _conv.ToId(it->key());

      // Must happen at some point when the queue is over the end
      if (id == corrector.head()) {
        if (!corrector.IsOverEnd())
          throw Exception(
            "Fatal logic failure: tail has reached the queue's head while the queue is not over the end",
            CurrentLocation);
        break;
      }

      // This `if` avoids a second check of IDs when the queue is over the end
      if (corrector.IsOverEnd() && corrector.IsTailMax() && id == 0
          && corrector.previous_checked_head() == 0) {
        corrector.SetTailToPrevious();
        break;
      }
      const auto next = corrector.FeedNext(id);

      // All good
      if (id == next) {
        continue;
      }

      // Seems like there was a forcefull terminataion, some writes were not complete.
      // We need to recover the queue's consistency by filling a gap in consecutive IDs.
      ShiftUp(it, id, next);
    }

    _head.store(corrector.head(), std::memory_order_relaxed);
    if (corrector.IsTailMax())
      _next_tail.store(0, std::memory_order_relaxed);
    else
      _next_tail.store(corrector.tail() + 1, std::memory_order_relaxed);
    if (Size() > GetMaxSize())
      throw Exception(
        "Fatal queue data state: the queue is too full, cannot execute operations on this queue",
        CurrentLocation);
  }

  PersistentQueue(PersistentQueue&& other)
    : _db(other._db), _head(other._head.load(std::memory_order_relaxed)),
      _next_tail(other._next_tail.load(std::memory_order_relaxed)) {}

#if defined(perq_WITH_STATS)
  Stats const& stats() { return _stats; };
#endif

  size_t Size() {
    const auto head = _head.load(std::memory_order_relaxed);
    const auto next_tail = _next_tail.load(std::memory_order_acquire);
    if (next_tail < head)
      return (_conv.GetMaxId() - head + 1) + next_tail;
    else
      return next_tail - head;
  }

  std::pair<std::string, bool> Top() {
    TKey head;
    TKey key;
    rocksdb::Slice slice;
    rocksdb::PinnableSlice pinned_value;
    rocksdb::Status status;
    auto count = decltype(_yield_after){0};

    perq_LocalStats;

    while (true) {
      head = _head.load(std::memory_order_relaxed);

      if (head == _next_tail.load(std::memory_order_acquire)) {
        perq_MergeLocalStatsForTop;
        return {"", false};
      }

      if (count == _yield_after) {
        perq_IncrementLocalYieldCount;
        count = 0;
        std::this_thread::yield();
      }
      ++count;

      key = _conv.ToKey(head);
      slice = ToSlice(&key);
      status = _db->Get(
        rocksdb::ReadOptions(), _db->DefaultColumnFamily(), slice, &pinned_value);

      // If we picked up a key that just was deleted
      if (status.IsNotFound()) {
        pinned_value.Reset();
        perq_IncrementLocalGetMissCount;
        continue;
      }

      if (!status.ok())
        throw Exception("Fatal error in RocksDB at `RocksDB::Get`: " + status.ToString(),
                        CurrentLocation);

      perq_MergeLocalStatsForTop;

      return {std::string(pinned_value.data(), pinned_value.size()), true};
    }
  }

  bool Pop() {
    TKey head;
    TKey new_head;
    TKey key;
    rocksdb::Slice slice;
    rocksdb::PinnableSlice pinned_value;
    rocksdb::Status status;
    auto count = decltype(_yield_after){0};

    head = _head.load(std::memory_order_relaxed);

    perq_LocalStats;

    do {
      perq_IncrementLocalCasRepetitionCount;

      if (head == _next_tail.load(std::memory_order_acquire)) {
        perq_MergeLocalStatsForPop;
        return false;
      }

      if (count == _yield_after) {
        perq_IncrementLocalYieldCount;
        count = 0;
        std::this_thread::yield();
      }
      ++count;

      pinned_value.Reset();

      if (head == _conv.GetMaxId())
        new_head = 0;
      else
        new_head = head + 1;

      key = _conv.ToKey(head);
      slice = ToSlice(&key);
      status = _db->Get(
        rocksdb::ReadOptions(), _db->DefaultColumnFamily(), slice, &pinned_value);

      // May happen in a case when `Push` has incremented `tail`, but
      // has not started/finished the write operation or when other `Poll` deleted it already
      if (status.IsNotFound()) {
        perq_IncrementLocalGetMissCount;
        continue;
      }

      if (!status.ok())
        throw Exception("Fatal error in RocksDB at `RocksDB::Get`: " + status.ToString(),
                        CurrentLocation);

    } while (!std::atomic_compare_exchange_weak_explicit(
      &_head, &head, new_head, std::memory_order_acquire, std::memory_order_acquire));

    pinned_value.Reset();
    status = _db->Delete(makeWriteOptions(), slice);
    if (!status.ok())
      throw Exception("Fatal error in RocksDB at `RocksDB::Delete`: " + status.ToString(),
                      CurrentLocation);

    perq_MergeLocalStatsForPop;

    return true;
  }

  std::pair<std::string, bool> Poll() {
    TKey head;
    TKey new_head;
    TKey key;
    rocksdb::Slice slice;
    rocksdb::PinnableSlice pinned_value;
    rocksdb::Status status;
    auto count = decltype(_yield_after){0};

    head = _head.load(std::memory_order_relaxed);

    perq_LocalStats;

    do {
      perq_IncrementLocalCasRepetitionCount;

      if (head == _next_tail.load(std::memory_order_acquire)) {
        perq_MergeLocalStatsForPoll;
        return {"", false};
      }

      if (count == _yield_after) {
        perq_IncrementLocalYieldCount;
        count = 0;
        std::this_thread::yield();
      }
      ++count;

      pinned_value.Reset();

      if (head == _conv.GetMaxId())
        new_head = 0;
      else
        new_head = head + 1;

      key = _conv.ToKey(head);
      slice = ToSlice(&key);
      status = _db->Get(
        rocksdb::ReadOptions(), _db->DefaultColumnFamily(), slice, &pinned_value);

      // May happen in a case when `Push` has incremented `tail`, but
      // has not started/finished the write operation or when other `Poll` deleted it already
      if (status.IsNotFound()) {
        perq_IncrementLocalGetMissCount;
        continue;
      }

      if (!status.ok())
        throw Exception("Fatal error in RocksDB at `RocksDB::Get`: " + status.ToString(),
                        CurrentLocation);

    } while (!std::atomic_compare_exchange_weak_explicit(
      &_head, &head, new_head, std::memory_order_acquire, std::memory_order_acquire));

    auto ret = std::pair<std::string, bool>(
      std::string(pinned_value.data(), pinned_value.size()), true);
    pinned_value.Reset();

    status = _db->Delete(makeWriteOptions(), slice);
    if (!status.ok())
      throw Exception("Fatal error in RocksDB at `RocksDB::Delete`: " + status.ToString(),
                      CurrentLocation);

    perq_MergeLocalStatsForPoll;

    return ret;
  }

  bool Push(const std::string& value) {
    TKey next_tail;
    TKey new_next_tail;
    auto count = decltype(_yield_after){0};

    next_tail = _next_tail.load(std::memory_order_relaxed);

    perq_LocalStats;

    do {
      perq_IncrementLocalCasRepetitionCount;

      if (count == _yield_after) {
        perq_IncrementLocalYieldCount;
        count = 0;
        std::this_thread::yield();
      }
      ++count;

      if (next_tail == _conv.GetMaxId())
        new_next_tail = 0;
      else
        new_next_tail = next_tail + 1;

      auto head = _head.load(std::memory_order_acquire);

      auto size = size_t{0};
      if (next_tail < head)
        size = (_conv.GetMaxId() - head + 1) + next_tail;
      else
        size = next_tail - head;

      if ((size + 1) >= GetMaxSize()) {
        perq_MergeLocalStatsForPush;
        return false;
      }
    } while (!std::atomic_compare_exchange_weak_explicit(&_next_tail,
                                                         &next_tail,
                                                         new_next_tail,
                                                         std::memory_order_acquire,
                                                         std::memory_order_acquire));

    next_tail = _conv.ToKey(next_tail);
    const auto status = _db->Put(makeWriteOptions(), ToSlice(&next_tail), value);
    if (!status.ok())
      throw Exception("Fatal error in RocksDB at `RocksDB::Put`: " + status.ToString(),
                      CurrentLocation);

    perq_MergeLocalStatsForPush;

    return true;
  }

private:
  void ShiftUp(std::unique_ptr<rocksdb::Iterator>& it, TKey from_id, TKey to_id) {
    auto from_key = _conv.ToKey(from_id);
    auto to_key = _conv.ToKey(to_id);
    Move(from_key, to_key);
    Seek(it, to_key);
    perq_IncrementShiftUpCount;
  }

  void Move(TKey sourceKey, TKey destinationKey) {
    auto sourceKeySlice = ToSlice(&sourceKey);
    auto destinationKeySlice = ToSlice(&destinationKey);
    std::string value;
    auto status = _db->Get(rocksdb::ReadOptions(), sourceKeySlice, &value);
    if (!status.ok())
      throw Exception("Fatal error in RocksDB at `RocksDB::Get`: " + status.ToString(),
                      CurrentLocation);
    rocksdb::WriteBatch batch;
    batch.Delete(sourceKeySlice);
    batch.Put(destinationKeySlice, value);
    rocksdb::WriteOptions write_options = {};
    write_options.sync = true;
    status = _db->Write(write_options, &batch);
    if (!status.ok())
      throw Exception("Fatal error in RocksDB at `RocksDB::Write`: " + status.ToString(),
                      CurrentLocation);
  }

  void Seek(std::unique_ptr<rocksdb::Iterator>& it, TKey key) {
    // Better to reset iterator, recommended by RocksDB development
    it.reset(_db->NewIterator(rocksdb::ReadOptions()));
    it->Seek(ToSlice(&key));
    if (!it->Valid())
      throw Exception("Fatal logic failure: failed to seek a key that must exist",
                      CurrentLocation);
  }

  rocksdb::Slice ToSlice(TKey* key) {
    return rocksdb::Slice(reinterpret_cast<char*>(key), sizeof(TKey));
  }

  rocksdb::WriteOptions makeWriteOptions() {
    rocksdb::WriteOptions options;

    // From RocksDB doc:
    // By default, each write to leveldb is asynchronous: it returns after pushing the
    // write from the process into the operating system. The transfer from operating
    // system memory to the underlying persistent storage happens asynchronously. The
    // sync flag can be turned on for a particular write to make the write operation
    // not return until the data being written has been pushed all the way to
    // persistent storage. (On Posix systems, this is implemented by calling either
    // fsync(...) or fdatasync(...) or msync(..., MS_SYNC) before the write operation
    // returns.)
    options.sync = false;

    return options;
  }

  size_t GetMaxSize() { return _conv.GetMaxId() - _max_thread_number + 1; }

  rocksdb::DB* _db;
  size_t _max_thread_number;
  std::atomic<TKey> _head;
  std::atomic<TKey> _next_tail;

#if defined(perq_WITH_STATS)
  Stats _stats = {};
#endif

  static constexpr PrefixedNumericalKeyConverter<TKey, TPrefix> _conv = {prefixValue};

  static constexpr size_t default_max_thread_number
    = (_conv.GetMaxId() > 100000) ? 100000 : 10000;

  static constexpr std::uint_fast8_t _yield_after = 10;
};

template <typename TKey,
          typename TPrefix,
          typename std::conditional<std::is_same<TPrefix, NoPrefix>::value,
                                    typename NoPrefix::Type,
                                    TPrefix>::type prefixValue>
constexpr PrefixedNumericalKeyConverter<TKey, TPrefix>
  PersistentQueue<TKey, TPrefix, prefixValue>::_conv;
}

#undef CurrentLocation
