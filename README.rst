perq - Persistent Queue
====================================

.. image:: https://travis-ci.org/ChoppinBlockParty/perq.svg?branch=master
    :target: https://travis-ci.org/ChoppinBlockParty/perq


Header-only persistent queue implementation based on RocksDB key-value storage.

Install
------

* Header-only usage
* C++-14 supporting compiler
* RocksDB
* Boost Filesystem, only for testing

How to
------

More examples could be found in ``tests/tests.cpp``.

.. code:: c++

    class MyQueueService {
    public:
      MyQueueService() {
        auto temp_db = (rocksdb::DB*){};
        rocksdb::Options options;
        options.create_if_missing = true;
        rocksdb::Status status = rocksdb::DB::Open(options, "/tmp/perq", &temp_db);
        if (!status.ok())
          throw std::runtime_error("Failed to open the storage");
        _db.reset(temp_db);
        _queue_a.Initialize(_db.get());
        _queue_b.Initialize(_db.get());
      }

      std::string Top(uint8_t topic) {
        switch (topic) {
        case 32:
          return _queue_a.Top().first;
        case 231:
          return _queue_b.Top().first;
        default:
          throw std::runtime_error("Invalid topic");
        }
      }

      void Pop(uint8_t topic) {
        switch (topic) {
        case 32:
          _queue_a.Pop();
          break;
        case 231:
          _queue_b.Pop();
          break;
        default:
          throw std::runtime_error("Invalid topic");
        }
      }

      std::string Poll(uint8_t topic) {
        switch (topic) {
        case 32:
          return _queue_a.Poll().first;
        case 231:
          return _queue_b.Poll().first;
        default:
          throw std::runtime_error("Invalid topic");
        }
      }

      void Push(uint8_t topic, std::string const& value) {
        switch (topic) {
        case 32:
          _queue_a.Push(value);
          break;
        case 231:
          _queue_b.Push(value);
          break;
        default:
          throw std::runtime_error("Invalid topic");
        }
      }

      std::unique_ptr<rocksdb::DB> _db;
      PersistentQueue<uint64_t, uint8_t, 32> _queue_a;
      PersistentQueue<uint64_t, uint8_t, 231> _queue_b;
    };

