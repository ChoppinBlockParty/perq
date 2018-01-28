#pragma once

#include <atomic>
#include <cstddef>

#if defined(perq_WITH_STATS) && !defined(preq_DISABLE_STATS_OPERATIONS)
#define perq_LocalStats LocalStats local_stats;
#define perq_IncrementLocalCasRepetitionCount ++local_stats.cas_repetition_count;
#define perq_IncrementLocalYieldCount ++local_stats.yield_count;
#define perq_IncrementLocalGetMissCount ++local_stats.get_miss_count;
#define perq_IncrementShiftUpCount ++_stats.shift_up_count;
#define perq_MergeLocalStatsForTop _stats.MergeLocalStatsForTop(local_stats);
#define perq_MergeLocalStatsForPop _stats.MergeLocalStatsForPop(local_stats);
#define perq_MergeLocalStatsForPoll _stats.MergeLocalStatsForPoll(local_stats);
#define perq_MergeLocalStatsForPush _stats.MergeLocalStatsForPush(local_stats);
#else
#define perq_LocalStats (void)0;
#define perq_IncrementLocalCasRepetitionCount (void)0;
#define perq_IncrementLocalYieldCount (void)0;
#define perq_IncrementLocalGetMissCount (void)0;
#define perq_IncrementShiftUpCount (void)0;
#define perq_MergeLocalStatsForTop (void)0;
#define perq_MergeLocalStatsForPop (void)0;
#define perq_MergeLocalStatsForPoll (void)0;
#define perq_MergeLocalStatsForPush (void)0;
#endif

namespace perq {
struct LocalStats {
  size_t cas_repetition_count = 0;
  size_t yield_count = 0;
  size_t get_miss_count = 0;
};

struct Stats {
  std::atomic<size_t> top_yield_count = {};
  std::atomic<size_t> top_get_miss_count = {};

  std::atomic<size_t> pop_cas_repetion_count = {};
  std::atomic<size_t> pop_yield_count = {};
  std::atomic<size_t> pop_get_miss_count = {};

  std::atomic<size_t> poll_cas_repetion_count = {};
  std::atomic<size_t> poll_yield_count = {};
  std::atomic<size_t> poll_get_miss_count = {};

  std::atomic<size_t> push_cas_repetion_count = {};
  std::atomic<size_t> push_yield_count = {};
  std::atomic<size_t> push_cas_repetion_max_count = {};
  std::atomic<size_t> push_cas_yield_max_count = {};

  std::atomic<size_t> shift_up_count = {};

  void MergeLocalStatsForTop(LocalStats const& stats) {
    top_yield_count += stats.yield_count;
    top_get_miss_count += stats.get_miss_count;
  }

  void MergeLocalStatsForPop(LocalStats const& stats) {
    if (stats.cas_repetition_count > 1)
      pop_cas_repetion_count += stats.cas_repetition_count - 1;
    pop_yield_count += stats.yield_count;
    pop_get_miss_count += stats.get_miss_count;
  }

  void MergeLocalStatsForPoll(LocalStats const& stats) {
    if (stats.cas_repetition_count > 1)
      poll_cas_repetion_count += stats.cas_repetition_count - 1;
    poll_yield_count += stats.yield_count;
    poll_get_miss_count += stats.get_miss_count;
  }

  void MergeLocalStatsForPush(LocalStats const& stats) {
    if (stats.cas_repetition_count > 1) {
      push_cas_repetion_count.fetch_add(stats.cas_repetition_count - 1,
                                        std::memory_order_relaxed);

      if ((stats.cas_repetition_count - 1)
          > push_cas_repetion_max_count.load(std::memory_order_relaxed))
        push_cas_repetion_max_count.store(stats.cas_repetition_count - 1,
                                          std::memory_order_relaxed);
    }

    push_yield_count.fetch_add(stats.yield_count, std::memory_order_relaxed);

    if (stats.yield_count > push_cas_yield_max_count.load(std::memory_order_relaxed))
      push_cas_yield_max_count.store(stats.cas_repetition_count,
                                     std::memory_order_relaxed);
  }
};

bool operator==(Stats const& lhs, Stats const& rhs) {
  return lhs.top_yield_count == rhs.top_yield_count
    && lhs.top_get_miss_count == rhs.top_get_miss_count
    && lhs.pop_cas_repetion_count == rhs.pop_cas_repetion_count
    && lhs.pop_yield_count == rhs.pop_yield_count
    && lhs.pop_get_miss_count == rhs.pop_get_miss_count
    && lhs.poll_cas_repetion_count == rhs.poll_cas_repetion_count
    && lhs.poll_yield_count == rhs.poll_yield_count
    && lhs.poll_get_miss_count == rhs.poll_get_miss_count
    && lhs.push_cas_repetion_count == rhs.push_cas_repetion_count
    && lhs.push_yield_count == rhs.push_yield_count
    && lhs.shift_up_count == rhs.shift_up_count;
}
}
