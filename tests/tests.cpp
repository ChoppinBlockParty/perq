#include <cstdint>
#include <iostream>
#include <random>
#include <string>

#include <boost/endian/conversion.hpp>
#include <boost/filesystem.hpp>

#include <rocksdb/db.h>

#define CATCH_CONFIG_MAIN
#include <catch/catch.hpp>

#define perq_WITH_STATS
// #define preq_DISABLE_STATS_OPERATIONS
#include <PersistentQueue.hpp>

namespace fs = boost::filesystem;

using namespace perq;

auto twister = std::mt19937(std::random_device()());
auto makeRandomNumber(uint32_t min, uint32_t max) -> uint32_t {
  auto distribution = std::uniform_int_distribution<uint32_t>(min, max);
  return distribution(twister);
}

auto makeRandomString(size_t size = makeRandomNumber(50, 3000)) -> std::string {
  auto ret = std::string(size, 0);
  for (size_t i = 0; i < size; ++i) {
    ret[i] = static_cast<char>(makeRandomNumber(0, 255));
  }
  return ret;
}

template <typename T>
bool IsEmpty(T& queue) {
  return !queue.Top().second && !queue.Pop() && !queue.Poll().second && queue.Size() == 0;
}

template <typename T>
bool IsSize(T& queue, size_t size) {
  return queue.Top().second && queue.Size() == size;
}

TEST_CASE("PrefixedNumericalKeyConverter", "[PrefixedNumericalKeyConverter]") {
  SECTION("32/16") {
    auto converter = PrefixedNumericalKeyConverter<uint32_t, uint16_t>(0x0001);

    REQUIRE(converter.GetMaxId() == 0x0000FFFF);
    uint32_t number = 0x00000000ul;
    uint32_t key = converter.ToKey(number);
    REQUIRE(key == boost::endian::native_to_big(static_cast<uint32_t>(0x00010000ul)));
    REQUIRE(converter.ToId(key) == number);
    number = 0x000000F0ul;
    key = converter.ToKey(number);
    REQUIRE(key == boost::endian::native_to_big(static_cast<uint32_t>(0x000100F0ul)));
    REQUIRE(converter.ToId(key) == number);
    number = 0x00000800ul;
    key = converter.ToKey(number);
    REQUIRE(key == boost::endian::native_to_big(static_cast<uint32_t>(0x00010800ul)));
    REQUIRE(converter.ToId(key) == number);
    number = 0x0000A000ul;
    key = converter.ToKey(number);
    REQUIRE(key == boost::endian::native_to_big(static_cast<uint32_t>(0x0001A000ul)));
    REQUIRE(converter.ToId(key) == number);
    number = 0x00008000ul;
    key = converter.ToKey(number);
    REQUIRE(key == boost::endian::native_to_big(static_cast<uint32_t>(0x00018000ul)));
    REQUIRE(converter.ToId(key) == number);
    number = 0xFFFFFFFFul;
    key = converter.ToKey(number);
    REQUIRE(key == boost::endian::native_to_big(static_cast<uint32_t>(0x0001FFFFul)));
    REQUIRE(converter.ToId(key) == 0x0000FFFFul);

    converter = PrefixedNumericalKeyConverter<uint32_t, uint16_t>(0x0F0F);

    REQUIRE(converter.GetMaxId() == 0x0000FFFFul);
    number = 0x00000000ul;
    key = converter.ToKey(number);
    REQUIRE(key == boost::endian::native_to_big(static_cast<uint32_t>(0x0F0F0000ul)));
    REQUIRE(converter.ToId(key) == number);
    number = 0x000000F0ul;
    key = converter.ToKey(number);
    REQUIRE(key == boost::endian::native_to_big(static_cast<uint32_t>(0x0F0F00F0ul)));
    REQUIRE(converter.ToId(key) == number);
    number = 0x00000800ul;
    key = converter.ToKey(number);
    REQUIRE(key == boost::endian::native_to_big(static_cast<uint32_t>(0x0F0F0800ul)));
    REQUIRE(converter.ToId(key) == number);
    number = 0x0000A000ul;
    key = converter.ToKey(number);
    REQUIRE(key == boost::endian::native_to_big(static_cast<uint32_t>(0x0F0FA000ul)));
    REQUIRE(converter.ToId(key) == number);
    number = 0x00008000ul;
    key = converter.ToKey(number);
    REQUIRE(key == boost::endian::native_to_big(static_cast<uint32_t>(0x0F0F8000ul)));
    REQUIRE(converter.ToId(key) == number);
    number = 0xFFFFFFFFul;
    key = converter.ToKey(number);
    REQUIRE(key == boost::endian::native_to_big(static_cast<uint32_t>(0x0F0FFFFFul)));
    REQUIRE(converter.ToId(key) == 0x0000FFFFul);

    converter = PrefixedNumericalKeyConverter<uint32_t, uint16_t>(0xFFFF);

    REQUIRE(converter.GetMaxId() == 0x0000FFFFul);
    number = 0x00000000ul;
    key = converter.ToKey(number);
    REQUIRE(key == boost::endian::native_to_big(static_cast<uint32_t>(0xFFFF0000ul)));
    REQUIRE(converter.ToId(key) == number);
    number = 0x000000F0ul;
    key = converter.ToKey(number);
    REQUIRE(key == boost::endian::native_to_big(static_cast<uint32_t>(0xFFFF00F0ul)));
    REQUIRE(converter.ToId(key) == number);
    number = 0x00000800ul;
    key = converter.ToKey(number);
    REQUIRE(key == boost::endian::native_to_big(static_cast<uint32_t>(0xFFFF0800ul)));
    REQUIRE(converter.ToId(key) == number);
    number = 0x0000A000ul;
    key = converter.ToKey(number);
    REQUIRE(key == boost::endian::native_to_big(static_cast<uint32_t>(0xFFFFA000ul)));
    REQUIRE(converter.ToId(key) == number);
    number = 0x00008000ul;
    key = converter.ToKey(number);
    REQUIRE(key == boost::endian::native_to_big(static_cast<uint32_t>(0xFFFF8000ul)));
    REQUIRE(converter.ToId(key) == number);
    number = 0xFFFFFFFFul;
    key = converter.ToKey(number);
    REQUIRE(key == boost::endian::native_to_big(static_cast<uint32_t>(0xFFFFFFFFul)));
    REQUIRE(converter.ToId(key) == 0x0000FFFF);
  }

  SECTION("64/8") {
    auto converter = PrefixedNumericalKeyConverter<uint64_t, uint8_t>(0x01);

    REQUIRE(converter.GetMaxId() == 0x00FFFFFFFFFFFFFFull);
    uint64_t number = 0x0000000000000000ull;
    uint64_t key = converter.ToKey(number);
    REQUIRE(
      key == boost::endian::native_to_big(static_cast<uint64_t>(0x0100000000000000ull)));
    REQUIRE(converter.ToId(key) == number);
    number = 0x00FFFFFFFFFFFFFFull;
    key = converter.ToKey(number);
    REQUIRE(
      key == boost::endian::native_to_big(static_cast<uint64_t>(0x01FFFFFFFFFFFFFFull)));
    REQUIRE(converter.ToId(key) == number);

    converter = PrefixedNumericalKeyConverter<uint64_t, uint8_t>(0x0F);

    REQUIRE(converter.GetMaxId() == 0x00FFFFFFFFFFFFFFull);
    number = 0x0000000000000000ull;
    key = converter.ToKey(number);
    REQUIRE(
      key == boost::endian::native_to_big(static_cast<uint64_t>(0x0F00000000000000ull)));
    REQUIRE(converter.ToId(key) == number);
    number = 0x00FFFFFFFFFFFFFFull;
    key = converter.ToKey(number);
    REQUIRE(
      key == boost::endian::native_to_big(static_cast<uint64_t>(0x0FFFFFFFFFFFFFFFull)));
    REQUIRE(converter.ToId(key) == number);

    converter = PrefixedNumericalKeyConverter<uint64_t, uint8_t>(0xFF);

    REQUIRE(converter.GetMaxId() == 0x00FFFFFFFFFFFFFFull);
    number = 0x0000000000000000ull;
    key = converter.ToKey(number);
    REQUIRE(
      key == boost::endian::native_to_big(static_cast<uint64_t>(0xFF00000000000000ull)));
    REQUIRE(converter.ToId(key) == number);
    number = 0x00FFFFFFFFFFFFFFull;
    key = converter.ToKey(number);
    REQUIRE(
      key == boost::endian::native_to_big(static_cast<uint64_t>(0xFFFFFFFFFFFFFFFFull)));
    REQUIRE(converter.ToId(key) == number);
  }

  SECTION("No prefix") {
    auto converter = PrefixedNumericalKeyConverter<uint64_t, NoPrefix>(0);

    REQUIRE(converter.GetMaxId() == 0xFFFFFFFFFFFFFFFFull);
    uint64_t number = 0x0000000000000000ull;
    uint64_t key = converter.ToKey(number);
    REQUIRE(
      key == boost::endian::native_to_big(static_cast<uint64_t>(0x0000000000000000ull)));
    REQUIRE(converter.ToId(key) == number);
    number = 0x00FFFFFFFFFFFFFFull;
    key = converter.ToKey(number);
    REQUIRE(
      key == boost::endian::native_to_big(static_cast<uint64_t>(0x00FFFFFFFFFFFFFFull)));
    REQUIRE(converter.ToId(key) == number);

    converter = PrefixedNumericalKeyConverter<uint64_t, NoPrefix>(0x0F);

    REQUIRE(converter.GetMaxId() == 0xFFFFFFFFFFFFFFFFull);
    number = 0x0000000000000000ull;
    key = converter.ToKey(number);
    REQUIRE(
      key == boost::endian::native_to_big(static_cast<uint64_t>(0x0000000000000000ull)));
    REQUIRE(converter.ToId(key) == number);
    number = 0x00FFFFFFFFFFFFFFull;
    key = converter.ToKey(number);
    REQUIRE(
      key == boost::endian::native_to_big(static_cast<uint64_t>(0x00FFFFFFFFFFFFFFull)));
    REQUIRE(converter.ToId(key) == number);

    converter = PrefixedNumericalKeyConverter<uint64_t, NoPrefix>(0xFF);

    REQUIRE(converter.GetMaxId() == 0xFFFFFFFFFFFFFFFFull);
    number = 0x0000000000000000ull;
    key = converter.ToKey(number);
    REQUIRE(
      key == boost::endian::native_to_big(static_cast<uint64_t>(0x0000000000000000ull)));
    REQUIRE(converter.ToId(key) == number);
    number = 0x00FFFFFFFFFFFFFFull;
    key = converter.ToKey(number);
    REQUIRE(
      key == boost::endian::native_to_big(static_cast<uint64_t>(0x00FFFFFFFFFFFFFFull)));
    REQUIRE(converter.ToId(key) == number);
  }
}

TEST_CASE("PersistentQueueIdCorrector basic", "[PersistentQueueIdCorrector][basic]") {
  auto corrector = PersistentQueueIdCorrector<uint16_t>(0, 255, 50);

  REQUIRE(!corrector.IsOverEnd());
  REQUIRE(corrector.head() == 0);
  REQUIRE(corrector.tail() == 0);
  REQUIRE(corrector.previous_checked_head() == std::numeric_limits<uint16_t>::max());
  REQUIRE(corrector.previous_checked_tail() == std::numeric_limits<uint16_t>::max());

  for (size_t i = 1; i < 256; ++i) {
    REQUIRE(corrector.FeedNext(i) == i);
    REQUIRE(!corrector.IsOverEnd());
    REQUIRE(corrector.head() == 0);
    REQUIRE(corrector.tail() == i);
  }

  REQUIRE_THROWS_WITH(
    corrector.FeedNext(256),
    "Severe misuse of `FeedNext`: the provided ID is greater than the maximum ID");

  for (size_t i = 0; i < 256; ++i) {
    REQUIRE_THROWS_WITH(
      corrector.FeedNext(i),
      "Severe misuse of `PersistentQueueIdCorrector::FeedNext`: the queue is not over the end, but next ID passes the end");
    REQUIRE(!corrector.IsOverEnd());
    REQUIRE(corrector.head() == 0);
    REQUIRE(corrector.tail() == 255);
  }

  corrector = PersistentQueueIdCorrector<uint16_t>(0, 255, 50);

  for (size_t i = 1; i < 20; ++i)
    corrector.FeedNext(i);

  for (size_t i = 70; i < 256; ++i) {
    REQUIRE(corrector.FeedNext(i) == i);
    REQUIRE(corrector.IsOverEnd());
    REQUIRE(corrector.head() == 70);
    REQUIRE(corrector.tail() == i);
  }

  for (size_t i = 0; i < 20; ++i) {
    REQUIRE(corrector.FeedNext(i) == i);
    REQUIRE(corrector.IsOverEnd());
    REQUIRE(corrector.head() == 70);
    REQUIRE(corrector.tail() == i);
  }

  REQUIRE_THROWS_WITH(
    corrector.FeedNext(70),
    "Severe misuse of `PersistentQueueIdCorrector::FeedNext`: the queue is over then end for the second time");
}

TEST_CASE("PersistentQueue basic", "[PersistentQueue][basic]") {
  auto temp_directory_path = fs::temp_directory_path() / "perq";
  if (fs::exists(temp_directory_path)) {
    fs::remove_all(temp_directory_path);
  }

  std::unique_ptr<rocksdb::DB> db;
  auto temp_db = (rocksdb::DB*){};
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::Status status
    = rocksdb::DB::Open(options, temp_directory_path.string(), &temp_db);
  if (!status.ok())
    REQUIRE(false);
  db.reset(temp_db);

  SECTION("New queue") {
    auto queue = PersistentQueue<uint64_t, uint8_t, 231>(db.get());
    REQUIRE(queue.stats() == Stats());

    // Emptiness test
    REQUIRE(IsEmpty(queue));

    // Push test
    auto value = makeRandomString();
    REQUIRE(queue.Push(value));
    REQUIRE(queue.Top() == std::pair<std::string, bool>(value, true));
    REQUIRE(IsSize(queue, 1));

    // Pop test
    REQUIRE(queue.Pop());
    REQUIRE(IsEmpty(queue));

    // Push test
    value = makeRandomString();
    REQUIRE(queue.Push(value));
    REQUIRE(queue.Top() == std::pair<std::string, bool>(value, true));
    REQUIRE(IsSize(queue, 1));

    // Poll test
    REQUIRE(queue.Poll() == std::pair<std::string, bool>(value, true));
    REQUIRE(IsEmpty(queue));

    // Twice Push and Poll test
    auto value1 = makeRandomString();
    REQUIRE(queue.Push(value1));
    REQUIRE(IsSize(queue, 1));
    REQUIRE(queue.Top() == std::pair<std::string, bool>(value1, true));
    auto value2 = makeRandomString();
    REQUIRE(queue.Push(value2));
    REQUIRE(IsSize(queue, 2));
    REQUIRE(queue.Top() == std::pair<std::string, bool>(value1, true));
    REQUIRE(queue.Poll() == std::pair<std::string, bool>(value1, true));
    REQUIRE(IsSize(queue, 1));
    REQUIRE(queue.Top() == std::pair<std::string, bool>(value2, true));
    REQUIRE(queue.Poll() == std::pair<std::string, bool>(value2, true));
    REQUIRE(IsEmpty(queue));

    // Twice Push and Pop test
    value1 = makeRandomString();
    REQUIRE(queue.Push(value1));
    REQUIRE(IsSize(queue, 1));
    REQUIRE(queue.Top() == std::pair<std::string, bool>(value1, true));
    value2 = makeRandomString();
    REQUIRE(queue.Push(value2));
    REQUIRE(IsSize(queue, 2));
    REQUIRE(queue.Top() == std::pair<std::string, bool>(value1, true));
    REQUIRE(queue.Pop());
    REQUIRE(IsSize(queue, 1));
    REQUIRE(queue.Top() == std::pair<std::string, bool>(value2, true));
    REQUIRE(queue.Pop());
    REQUIRE(IsEmpty(queue));

    // Fill the queue
    for (size_t i = 0; i < 100; ++i) {
      value = makeRandomString();
      REQUIRE(queue.Push(value));
      REQUIRE(IsSize(queue, i + 1));
    }
    REQUIRE(queue.stats() == Stats());
  }

  {
    auto queue = PersistentQueue<uint64_t, uint8_t, 231>(db.get());
    for (size_t i = 0; i < 100; ++i) {
      queue.Push(makeRandomString());
    }

    db.reset(nullptr);
    status = rocksdb::DB::Open(options, temp_directory_path.string(), &temp_db);
    if (!status.ok())
      REQUIRE(false);

    db.reset(temp_db);

    REQUIRE(queue.stats() == Stats());
  }

  SECTION("Existing queue") {
    auto queue = PersistentQueue<uint64_t, uint8_t, 231>(db.get());

    REQUIRE(IsSize(queue, 100));

    REQUIRE(queue.Poll().second);
    REQUIRE(IsSize(queue, 99));
    auto value1 = makeRandomString();
    REQUIRE(queue.Push(value1));
    REQUIRE(IsSize(queue, 100));
    auto value2 = makeRandomString();
    REQUIRE(queue.Push(value2));
    REQUIRE(IsSize(queue, 101));
    REQUIRE(queue.Pop());
    REQUIRE(IsSize(queue, 100));
    REQUIRE(queue.Pop());
    REQUIRE(IsSize(queue, 99));

    REQUIRE(queue.stats() == Stats());
  }

  {
    auto queue = PersistentQueue<uint64_t, uint8_t, 231>(db.get());
    while (queue.Pop())
      ;
    for (size_t i = 0; i < 99; ++i) {
      queue.Push(makeRandomString());
    }

    db.reset(nullptr);
    status = rocksdb::DB::Open(options, temp_directory_path.string(), &temp_db);
    if (!status.ok())
      REQUIRE(false);

    db.reset(temp_db);

    REQUIRE(queue.stats() == Stats());
  }

  SECTION("No-prefix queue") {
    // This is just a test, never do any similar on real data: do not change the queue
    // type
    auto queue = PersistentQueue<uint64_t>(db.get());
    REQUIRE(IsSize(queue, 99));

    for (size_t i = 0; i < 99; ++i) {
      REQUIRE(queue.Poll().second);
      REQUIRE(queue.Size() == 98 - i);
    }

    // Emptiness test
    REQUIRE(!queue.Top().second);
    REQUIRE(!queue.Pop());
    REQUIRE(!queue.Poll().second);
    REQUIRE(!queue.Size());

    // Push test
    auto value = makeRandomString();
    REQUIRE(queue.Push(value));
    REQUIRE(queue.Top() == std::pair<std::string, bool>(value, true));
    REQUIRE(queue.Size() == 1);

    // Pop test
    REQUIRE(queue.Pop());
    REQUIRE(!queue.Top().second);
    REQUIRE(!queue.Pop());
    REQUIRE(!queue.Poll().second);
    REQUIRE(!queue.Size());

    // Push test
    value = makeRandomString();
    REQUIRE(queue.Push(value));
    REQUIRE(queue.Top() == std::pair<std::string, bool>(value, true));
    REQUIRE(queue.Size() == 1);

    // Poll test
    REQUIRE(queue.Poll() == std::pair<std::string, bool>(value, true));
    REQUIRE(!queue.Top().second);
    REQUIRE(!queue.Pop());
    REQUIRE(!queue.Poll().second);
    REQUIRE(!queue.Size());

    REQUIRE(queue.stats() == Stats());
  }
}

TEST_CASE("PersistentQueue parallel", "[PersistentQueue][parallel]") {
  auto temp_directory_path = fs::temp_directory_path() / "perq";
  if (fs::exists(temp_directory_path)) {
    fs::remove_all(temp_directory_path);
  }

  std::unique_ptr<rocksdb::DB> db;
  auto temp_db = (rocksdb::DB*){};
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::Status status
    = rocksdb::DB::Open(options, temp_directory_path.string(), &temp_db);
  if (!status.ok())
    REQUIRE(false);
  db.reset(temp_db);

  SECTION("Paralle Top") {
    auto queue = PersistentQueue<uint64_t, uint8_t, 231>(db.get());
    REQUIRE(IsEmpty(queue));
    {
      std::thread thread_one([&]() {
        for (int i = 0; i < 50000; ++i)
          queue.Push("small");
      });
      std::thread thread_two([&]() {
        for (int i = 0; i < 50000; ++i)
          queue.Push("small 2");
      });
      thread_one.join();
      thread_two.join();
    }
    REQUIRE(IsSize(queue, 100000));

    std::thread thread_one([&]() {
      for (int i = 0; i < 50000; ++i)
        queue.Top();
    });
    std::thread thread_two([&]() {
      for (int i = 0; i < 50000; ++i)
        queue.Top();
    });
    thread_one.join();
    thread_two.join();

    REQUIRE(IsSize(queue, 100000));

    REQUIRE(queue.stats().top_get_miss_count == 0);
    REQUIRE(queue.stats().top_yield_count == 0);

    while (queue.Pop())
      ;
  }

  SECTION("Paralle Pop") {
    auto queue = PersistentQueue<uint64_t, uint8_t, 231>(db.get());
    REQUIRE(IsEmpty(queue));
    {
      std::thread thread_one([&]() {
        for (int i = 0; i < 50000; ++i)
          queue.Push("small");
      });
      std::thread thread_two([&]() {
        for (int i = 0; i < 50000; ++i)
          queue.Push("small 2");
      });
      thread_one.join();
      thread_two.join();
    }
    REQUIRE(IsSize(queue, 100000));

    std::thread thread_one([&]() {
      for (int i = 0; i < 50000; ++i)
        queue.Pop();
    });
    std::thread thread_two([&]() {
      for (int i = 0; i < 50000; ++i)
        queue.Pop();
    });
    thread_one.join();
    thread_two.join();
    REQUIRE(IsEmpty(queue));

    std::cerr << "Pop cas repetition count: " << queue.stats().pop_cas_repetion_count
              << std::endl;
    std::cerr << "Pop yield count: " << queue.stats().pop_yield_count << std::endl;
    std::cerr << "Pop get miss count: " << queue.stats().pop_get_miss_count << std::endl;
  }

  SECTION("Paralle Poll") {
    auto queue = PersistentQueue<uint64_t, uint8_t, 231>(db.get());
    REQUIRE(IsEmpty(queue));
    {
      std::thread thread_one([&]() {
        for (int i = 0; i < 50000; ++i)
          queue.Push("small");
      });
      std::thread thread_two([&]() {
        for (int i = 0; i < 50000; ++i)
          queue.Push("small 2");
      });
      thread_one.join();
      thread_two.join();
    }
    REQUIRE(IsSize(queue, 100000));

    std::thread thread_one([&]() {
      for (int i = 0; i < 50000; ++i)
        queue.Poll();
    });
    std::thread thread_two([&]() {
      for (int i = 0; i < 50000; ++i)
        queue.Poll();
    });
    thread_one.join();
    thread_two.join();
    REQUIRE(IsEmpty(queue));

    std::cerr << "Poll cas repetition count: " << queue.stats().poll_cas_repetion_count
              << std::endl;
    std::cerr << "Poll yield count: " << queue.stats().poll_yield_count << std::endl;
    std::cerr << "Poll get miss count: " << queue.stats().poll_get_miss_count
              << std::endl;
  }

  SECTION("Paralle Push") {
    auto queue = PersistentQueue<uint64_t, uint8_t, 231>(db.get());
    REQUIRE(IsEmpty(queue));

    std::thread thread_one([&]() {
      for (int i = 0; i < 50000; ++i)
        queue.Push("small");
    });
    std::thread thread_two([&]() {
      for (int i = 0; i < 50000; ++i)
        queue.Push("small 2");
    });
    thread_one.join();
    thread_two.join();

    REQUIRE(IsSize(queue, 100000));

    std::cerr << "Push cas repetition count: " << queue.stats().push_cas_repetion_count
              << std::endl;
    std::cerr << "Push yield count: " << queue.stats().push_yield_count << std::endl;
    std::cerr << "Push cas repetition max count: "
              << queue.stats().push_cas_repetion_max_count << std::endl;
    std::cerr << "Push cas yield max count: " << queue.stats().push_cas_yield_max_count
              << std::endl;

    while (queue.Pop())
      ;
  }

  SECTION("Paralle Push And Pop") {
    auto queue = PersistentQueue<uint64_t, uint8_t, 231>(db.get());
    REQUIRE(IsEmpty(queue));

    std::thread thread_one([&]() {
      for (int i = 0; i < 50000; ++i)
        queue.Push("small");
    });
    std::thread thread_two([&]() {
      for (int i = 0; i < 50000;) {
        if (queue.Pop())
          ++i;
      }
    });
    thread_one.join();
    thread_two.join();

    REQUIRE(IsEmpty(queue));

    std::cerr << "Pop cas repetition count: " << queue.stats().pop_cas_repetion_count
              << std::endl;
    std::cerr << "Pop yield count: " << queue.stats().pop_yield_count << std::endl;
    std::cerr << "Pop get miss count: " << queue.stats().pop_get_miss_count << std::endl;
    std::cerr << "Push cas repetition count: " << queue.stats().push_cas_repetion_count
              << std::endl;
    std::cerr << "Push yield count: " << queue.stats().push_yield_count << std::endl;
    std::cerr << "Push cas repetition max count: "
              << queue.stats().push_cas_repetion_max_count << std::endl;
    std::cerr << "Push cas yield max count: " << queue.stats().push_cas_yield_max_count
              << std::endl;
  }

  SECTION("Paralle Push And Poll") {
    auto queue = PersistentQueue<uint64_t, uint8_t, 231>(db.get());
    REQUIRE(IsEmpty(queue));

    std::thread thread_one([&]() {
      for (int i = 0; i < 50000; ++i)
        queue.Push("small");
    });
    std::thread thread_two([&]() {
      for (int i = 0; i < 50000;) {
        if (queue.Poll().second)
          ++i;
      }
    });
    thread_one.join();
    thread_two.join();

    REQUIRE(IsEmpty(queue));

    std::cerr << "Poll cas repetition count: " << queue.stats().poll_cas_repetion_count
              << std::endl;
    std::cerr << "Poll yield count: " << queue.stats().poll_yield_count << std::endl;
    std::cerr << "Poll get miss count: " << queue.stats().poll_get_miss_count
              << std::endl;

    REQUIRE(queue.stats().push_cas_repetion_count == 0);
    REQUIRE(queue.stats().push_yield_count == 0);
    REQUIRE(queue.stats().push_cas_repetion_max_count == 0);
    REQUIRE(queue.stats().push_cas_yield_max_count == 0);
  }

  SECTION("Paralle Push And Poll And Size") {
    auto queue = PersistentQueue<uint64_t, uint8_t, 231>(db.get());
    REQUIRE(IsEmpty(queue));

    std::atomic_bool is_running(true);

    std::thread thread_one([&]() {
      for (int i = 0; i < 50000; ++i)
        queue.Push("small");
    });
    std::thread thread_two([&]() {
      for (int i = 0; i < 50000;) {
        if (queue.Poll().second)
          ++i;
      }
      is_running = false;
    });
    std::thread thread_three([&]() {
      for (int i = 0; is_running; ++i) {
        if (queue.Size() > 50000)
          throw std::runtime_error("Invalid size");
      }
    });
    thread_one.join();
    thread_two.join();
    thread_three.join();

    REQUIRE(IsEmpty(queue));

    REQUIRE(queue.stats().push_cas_repetion_count == 0);
    REQUIRE(queue.stats().push_yield_count == 0);
    REQUIRE(queue.stats().push_cas_repetion_max_count == 0);
    REQUIRE(queue.stats().push_cas_yield_max_count == 0);
  }
}
