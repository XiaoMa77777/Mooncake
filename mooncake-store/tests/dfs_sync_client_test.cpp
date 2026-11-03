#include <gtest/gtest.h>

#include <atomic>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <memory>
#include <optional>
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <unistd.h>

#include "client_service.h"
#include "storage/distributed/dfs_global_allocator.h"
#include "storage/distributed/distributed_storage_backend.h"
#include "storage/distributed/posix_fs_adapter.h"
#include "test_server_helpers.h"
#include "utils.h"

namespace mooncake::test {

class FailingPosixFsAdapter : public PosixFsAdapter {
   public:
    int WriteCalls() const { return write_calls_.load(); }
    void FailWriteCall(int call) { fail_write_call_.store(call); }

    tl::expected<size_t, ErrorCode> WriteAt(int fd, const iovec* iov,
                                            int iovcnt,
                                            int64_t offset) override {
        const int call = ++write_calls_;
        if (call == fail_write_call_.load()) {
            return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }
        return PosixFsAdapter::WriteAt(fd, iov, iovcnt, offset);
    }

   private:
    std::atomic<int> write_calls_{0};
    std::atomic<int> fail_write_call_{-1};
};

class DfsSyncClientTest : public ::testing::Test {
   protected:
    void SetUp() override {
        root_ = (std::filesystem::temp_directory_path() /
                 ("dfs_sync_client_" + std::to_string(::getpid()) + "_" +
                  std::to_string(++next_root_)))
                    .string();
        std::filesystem::create_directories(root_);

        SetEnv("MOONCAKE_ENABLE_DFS", "1");
        SetEnv("MOONCAKE_DFS_FS_ADAPTER", "posix");
        SetEnv("MOONCAKE_DFS_ROOT_DIR", root_);
        // Functional cases want a small allocator; a timed one needs a larger
        // one, because the DFS allocator (not the MEMORY segment) is what caps
        // how many live objects the stack can hold, and it has no eviction.
        const char* shard_count_text = std::getenv("MOONCAKE_STACK_DFS_SHARD_COUNT");
        const char* shard_cap_text =
            std::getenv("MOONCAKE_STACK_DFS_SHARD_CAPACITY");
        dfs_shard_count_ =
            shard_count_text != nullptr ? std::stoull(shard_count_text) : 2;
        dfs_shard_capacity_ = shard_cap_text != nullptr
                                  ? std::stoull(shard_cap_text)
                                  : (16ull * 1024ull * 1024ull);
        SetEnv("MOONCAKE_DFS_SHARD_COUNT", std::to_string(dfs_shard_count_));
        SetEnv("MOONCAKE_DFS_SHARD_CAPACITY",
               std::to_string(dfs_shard_capacity_));
        SetEnv("MOONCAKE_DFS_ALIGNMENT", "4096");
        SetEnv("MOONCAKE_DFS_EVICTION_ENABLED", "0");
        SetEnv("MOONCAKE_DFS_DEFERRED_FREE_SECONDS", "0");
        SetEnv("MOONCAKE_DFS_SINGLE_TENANT", "true");

        ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder().build()));
        writer_ = CreateClient("127.0.0.1:18101");
        provider_ = CreateClient("127.0.0.1:18102");
        ASSERT_NE(writer_, nullptr);
        ASSERT_NE(provider_, nullptr);

        segment_size_ = 16 * 1024 * 1024;
        segment_ = allocate_buffer_allocator_memory(segment_size_);
        ASSERT_NE(segment_, nullptr);
        ASSERT_TRUE(provider_->MountSegment(segment_, segment_size_, "tcp")
                        .has_value());

        FileStorageConfig file_config;
        file_config.storage_backend_type = StorageBackendType::kDistributed;
        file_config.storage_filepath = root_;

        DistributedStorageConfig distributed_config;
        distributed_config.fsdir = root_;
        distributed_config.fs_adapter_type = "posix";
        distributed_config.shard_count = static_cast<std::size_t>(dfs_shard_count_);
        distributed_config.shard_capacity = static_cast<std::size_t>(dfs_shard_capacity_);
        distributed_config.alignment = 4096;

        auto adapter = std::make_unique<FailingPosixFsAdapter>();
        adapter_ = adapter.get();
        backend_ = std::make_shared<DistributedStorageBackend>(
            file_config, distributed_config, std::move(adapter));
        ASSERT_TRUE(backend_->Init().has_value());
        writer_->SetDfsStorageBackend(backend_);
    }

    // Set by SetUp from the optional stack-capacity overrides.
    std::uint64_t dfs_shard_count_ = 2;
    std::uint64_t dfs_shard_capacity_ = 16 * 1024 * 1024;

    void TearDown() override {
        if (provider_ && segment_) {
            (void)provider_->UnmountSegment(segment_, segment_size_);
        }
        writer_.reset();
        provider_.reset();
        backend_.reset();
        master_.Stop();
        if (segment_) {
            std::free(segment_);
            segment_ = nullptr;
        }
        RestoreEnv();
        std::error_code ec;
        std::filesystem::remove_all(root_, ec);
    }

    std::shared_ptr<Client> CreateClient(const std::string& hostname) {
        auto client = Client::Create(hostname, "P2PHANDSHAKE", "tcp",
                                     std::nullopt, master_.master_address());
        return client ? *client : nullptr;
    }

    ReplicateConfig DfsConfig() const {
        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;
        return config;
    }

    tl::expected<QueryResult, ErrorCode> QueryDfsOnly(const std::string& key) {
        auto query = writer_->Query(key);
        if (!query) {
            return tl::make_unexpected(query.error());
        }
        std::vector<Replica::Descriptor> replicas;
        for (const auto& replica : query->replicas) {
            if (replica.is_dfs_replica()) {
                replicas.push_back(replica);
            }
        }
        if (replicas.empty()) {
            return tl::make_unexpected(ErrorCode::INVALID_REPLICA);
        }
        return QueryResult(std::move(replicas), query->lease_timeout,
                           query->object_checksum);
    }

    void ExpectDfsValue(const std::string& key, const std::string& expected) {
        auto query = QueryDfsOnly(key);
        ASSERT_TRUE(query.has_value());
        std::vector<char> output(expected.size());
        const auto& descriptor = query->replicas[0].get_dfs_descriptor();
        auto results = backend_->BatchRead(
            {{key, descriptor, {{output.data(), output.size()}}}});
        ASSERT_EQ(results.size(), 1);
        ASSERT_TRUE(results[0].has_value());
        EXPECT_EQ(std::memcmp(output.data(), expected.data(), expected.size()),
                  0);
    }

    static std::vector<std::vector<Slice>> MakeSlices(
        std::vector<std::string>& values) {
        std::vector<std::vector<Slice>> slices;
        slices.reserve(values.size());
        for (auto& value : values) {
            slices.push_back({Slice{value.data(), value.size()}});
        }
        return slices;
    }

    void SetEnv(const std::string& key, const std::string& value) {
        const char* old_value = ::getenv(key.c_str());
        saved_env_.push_back({key, old_value
                                       ? std::optional<std::string>(old_value)
                                       : std::nullopt});
        ::setenv(key.c_str(), value.c_str(), 1);
    }

    void RestoreEnv() {
        for (auto it = saved_env_.rbegin(); it != saved_env_.rend(); ++it) {
            if (it->second) {
                ::setenv(it->first.c_str(), it->second->c_str(), 1);
            } else {
                ::unsetenv(it->first.c_str());
            }
        }
        saved_env_.clear();
    }

    inline static std::atomic<int> next_root_{0};
    testing::InProcMaster master_;
    std::shared_ptr<Client> writer_;
    std::shared_ptr<Client> provider_;
    std::shared_ptr<DistributedStorageBackend> backend_;
    FailingPosixFsAdapter* adapter_ = nullptr;
    void* segment_ = nullptr;
    size_t segment_size_ = 0;
    std::string root_;
    std::vector<std::pair<std::string, std::optional<std::string>>> saved_env_;
};

TEST_F(DfsSyncClientTest, PutAndUpsertReturnAfterDfsWrite) {
    const std::string key = "sync_put_upsert";
    std::string initial(4096, 'A');
    std::vector<Slice> initial_slices{{initial.data(), initial.size()}};
    ASSERT_TRUE(writer_->Put(key, initial_slices, DfsConfig()).has_value());
    ExpectDfsValue(key, initial);

    std::string updated(4096, 'B');
    std::vector<Slice> updated_slices{{updated.data(), updated.size()}};
    ASSERT_TRUE(writer_->Upsert(key, updated_slices, DfsConfig()).has_value());
    ExpectDfsValue(key, updated);
}

TEST_F(DfsSyncClientTest, BatchPutAndBatchUpsertReturnAfterDfsWrite) {
    std::vector<std::string> keys{"sync_batch_0", "sync_batch_1"};
    std::vector<std::string> initial{std::string(4096, 'C'),
                                     std::string(4096, 'D')};
    auto initial_slices = MakeSlices(initial);
    auto put_results = writer_->BatchPut(keys, initial_slices, DfsConfig());
    ASSERT_EQ(put_results.size(), keys.size());
    ASSERT_TRUE(put_results[0].has_value());
    ASSERT_TRUE(put_results[1].has_value());
    ExpectDfsValue(keys[0], initial[0]);
    ExpectDfsValue(keys[1], initial[1]);

    std::vector<std::string> updated{std::string(4096, 'E'),
                                     std::string(4096, 'F')};
    auto updated_slices = MakeSlices(updated);
    auto upsert_results =
        writer_->BatchUpsert(keys, updated_slices, DfsConfig());
    ASSERT_EQ(upsert_results.size(), keys.size());
    ASSERT_TRUE(upsert_results[0].has_value());
    ASSERT_TRUE(upsert_results[1].has_value());
    ExpectDfsValue(keys[0], updated[0]);
    ExpectDfsValue(keys[1], updated[1]);
}

TEST_F(DfsSyncClientTest, PreferSameNodeBatchPutReturnsAfterDfsWrite) {
    std::vector<std::string> keys{"sync_same_node_0", "sync_same_node_1"};
    std::vector<std::string> values{std::string(4096, 'J'),
                                    std::string(4096, 'K')};
    auto slices = MakeSlices(values);
    auto config = DfsConfig();
    config.prefer_alloc_in_same_node = true;

    auto results = writer_->BatchPut(keys, slices, config);
    ASSERT_EQ(results.size(), keys.size());
    ASSERT_TRUE(results[0].has_value());
    ASSERT_TRUE(results[1].has_value());
    ExpectDfsValue(keys[0], values[0]);
    ExpectDfsValue(keys[1], values[1]);
}

TEST_F(DfsSyncClientTest, BatchDfsFailureOnlyRevokesFailedKey) {
    std::vector<std::string> keys{"sync_success", "sync_failure"};
    std::vector<std::string> values{std::string(4096, 'G'),
                                    std::string(4096, 'H')};
    auto slices = MakeSlices(values);
    adapter_->FailWriteCall(adapter_->WriteCalls() + 2);

    auto results = writer_->BatchPut(keys, slices, DfsConfig());
    ASSERT_EQ(results.size(), keys.size());
    EXPECT_TRUE(results[0].has_value());
    ASSERT_FALSE(results[1].has_value());
    EXPECT_EQ(results[1].error(), ErrorCode::FILE_WRITE_FAIL);
    ExpectDfsValue(keys[0], values[0]);

    auto failed_query = writer_->Query(keys[1]);
    ASSERT_FALSE(failed_query.has_value());
    EXPECT_EQ(failed_query.error(), ErrorCode::OBJECT_NOT_FOUND);
}

TEST_F(DfsSyncClientTest, MissingDfsBackendRevokesObject) {
    auto client_without_backend = CreateClient("127.0.0.1:18103");
    ASSERT_NE(client_without_backend, nullptr);
    std::string value(4096, 'I');
    std::vector<Slice> slices{{value.data(), value.size()}};

    auto result =
        client_without_backend->Put("sync_no_backend", slices, DfsConfig());
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::DFS_SERVICE_UNAVAILABLE);
    auto query = client_without_backend->Query("sync_no_backend");
    ASSERT_FALSE(query.has_value());
    EXPECT_EQ(query.error(), ErrorCode::OBJECT_NOT_FOUND);
}

TEST_F(DfsSyncClientTest, GetReadsDfsIntoMultipleSlices) {
    const std::string key = "dfs_multi_slice_get";
    std::string value(4096, 'M');
    std::vector<Slice> write_slices{{value.data(), value.size()}};
    ASSERT_TRUE(writer_->Put(key, write_slices, DfsConfig()).has_value());

    auto query = QueryDfsOnly(key);
    ASSERT_TRUE(query.has_value());
    std::vector<char> first(1024), second(3072);
    std::vector<Slice> read_slices{{first.data(), first.size()},
                                   {second.data(), second.size()}};
    ASSERT_TRUE(writer_->Get(key, *query, read_slices).has_value());
    EXPECT_EQ(std::memcmp(value.data(), first.data(), first.size()), 0);
    EXPECT_EQ(
        std::memcmp(value.data() + first.size(), second.data(), second.size()),
        0);
}

TEST_F(DfsSyncClientTest, BatchGetUsesExplicitDfsDescriptors) {
    std::vector<std::string> keys{"dfs_batch_read_0", "dfs_batch_read_1"};
    std::vector<std::string> values{std::string(4096, 'N'),
                                    std::string(4096, 'O')};
    auto write_slices = MakeSlices(values);
    auto put_results = writer_->BatchPut(keys, write_slices, DfsConfig());
    ASSERT_EQ(put_results.size(), keys.size());
    ASSERT_TRUE(put_results[0].has_value());
    ASSERT_TRUE(put_results[1].has_value());

    std::vector<QueryResult> queries;
    queries.reserve(keys.size());
    for (const auto& key : keys) {
        auto query = QueryDfsOnly(key);
        ASSERT_TRUE(query.has_value());
        queries.push_back(*query);
    }

    std::vector<std::string> output{std::string(4096, '\0'),
                                    std::string(4096, '\0')};
    std::unordered_map<std::string, std::vector<Slice>> read_slices;
    for (size_t i = 0; i < keys.size(); ++i) {
        read_slices[keys[i]] = {{output[i].data(), output[i].size()}};
    }
    auto get_results = writer_->BatchGet(keys, queries, read_slices);
    ASSERT_EQ(get_results.size(), keys.size());
    ASSERT_TRUE(get_results[0].has_value());
    ASSERT_TRUE(get_results[1].has_value());
    EXPECT_EQ(output[0], values[0]);
    EXPECT_EQ(output[1], values[1]);
}

TEST_F(DfsSyncClientTest, BatchGetVerifiesDfsChecksum) {
    const char* checksum_enabled = std::getenv("MOONCAKE_STORE_CHECKSUM");
    if (checksum_enabled == nullptr || std::string(checksum_enabled) != "1") {
        GTEST_SKIP() << "MOONCAKE_STORE_CHECKSUM is not enabled";
    }

    const std::string key = "dfs_batch_checksum";
    std::string value(4096, 'P');
    std::vector<Slice> write_slices{{value.data(), value.size()}};
    ASSERT_TRUE(writer_->Put(key, write_slices, DfsConfig()).has_value());

    auto query = QueryDfsOnly(key);
    ASSERT_TRUE(query.has_value());
    ASSERT_TRUE(query->object_checksum.has_value());
    std::vector<Replica::Descriptor> replicas = query->replicas;
    std::vector<QueryResult> queries;
    queries.emplace_back(
        std::move(replicas), query->lease_timeout,
        std::optional<uint64_t>(*query->object_checksum ^ uint64_t{1}));

    std::string output(value.size(), '\0');
    std::unordered_map<std::string, std::vector<Slice>> read_slices;
    read_slices[key] = {{output.data(), output.size()}};
    auto results = writer_->BatchGet({key}, queries, read_slices);
    ASSERT_EQ(results.size(), 1);
    ASSERT_FALSE(results[0].has_value());
    EXPECT_EQ(results[0].error(), ErrorCode::CHECKSUM_MISMATCH);
}


#ifdef MOONCAKE_WITH_KVCLIENT_OBJECT
// HOST B2: object mode through the real client factory, read back through the
// real Client/Master path with MEMORY hits excluded.
//
// Opt-in by environment, because the case needs an isolated SDK/backend and a
// runner that can clean up after it. A plain suite run skips rather than
// pretending: the acceptance runner must show 1 executed / 1 passed / 0 skipped,
// so a skip here is a failed run, not a pass.
TEST_F(DfsSyncClientTest, ObjectStorageModeBuildsThroughTheRealFactory) {
    const char* sdk_config = std::getenv("MOONCAKE_B2_KVCLIENT_CONFIG");
    const char* instance = std::getenv("MOONCAKE_B2_INSTANCE");
    const char* epoch = std::getenv("MOONCAKE_B2_EPOCH");
    const char* run_id = std::getenv("MOONCAKE_B2_RUN_ID");
    if (sdk_config == nullptr || instance == nullptr || epoch == nullptr ||
        run_id == nullptr) {
        GTEST_SKIP() << "HOST B2 needs MOONCAKE_B2_KVCLIENT_CONFIG, "
                        "MOONCAKE_B2_INSTANCE, MOONCAKE_B2_EPOCH and "
                        "MOONCAKE_B2_RUN_ID";
    }

    // The Master above was started on a posix allocator; only the client switches
    // to object_storage. That split is what ValidateForAllocator enforces.
    SetEnv("MOONCAKE_DFS_FS_ADAPTER", "object_storage");
    SetEnv("MOONCAKE_DFS_OBJECT_CACHE_INSTANCE", instance);
    SetEnv("MOONCAKE_DFS_OBJECT_EPOCH", epoch);
    SetEnv("MOONCAKE_DFS_KVCLIENT_SDK_CONFIG", sdk_config);
    SetEnv("MOONCAKE_DFS_KVCLIENT_TIMEOUT_MS", "15000");

    FileStorageConfig file_config;
    file_config.storage_backend_type = StorageBackendType::kDistributed;
    file_config.storage_filepath = root_;

    auto created = CreateStorageBackend(file_config);
    ASSERT_TRUE(created.has_value())
        << "the real factory refused an object_storage configuration";

    auto object_backend =
        std::dynamic_pointer_cast<DistributedStorageBackend>(*created);
    ASSERT_NE(object_backend, nullptr);
    ASSERT_TRUE(object_backend->UsesObjectStorage());
    auto object_init = object_backend->Init();
    ASSERT_TRUE(object_init.has_value())
        << "object backend Init failed, error=" << static_cast<int>(object_init.error());

    backend_ = object_backend;
    writer_->SetDfsStorageBackend(backend_);

    // Read back into two slices through the Client, so the DFS descriptor comes
    // from the Master directory rather than from the test.
    // The runner knows these three keys in advance and deletes them after the
    // timed window, so the first one is `-single` by agreement, not by accident.
    const std::string key = std::string(run_id) + "-single";
    const std::string value(4096, 'M');
    // Two slices, so the write really is multi-region rather than one buffer
    // pretending to be two.
    std::vector<Slice> write_slices{{const_cast<char*>(value.data()), 2048},
                                    {const_cast<char*>(value.data()) + 2048,
                                     2048}};
    ASSERT_TRUE(writer_->Put(key, write_slices, DfsConfig()).has_value());

    auto query = QueryDfsOnly(key);
    ASSERT_TRUE(query.has_value());
    std::vector<char> first(1024), second(3072);
    std::vector<Slice> read_slices{{first.data(), first.size()},
                                   {second.data(), second.size()}};
    ASSERT_TRUE(writer_->Get(key, *query, read_slices).has_value());
    EXPECT_EQ(std::memcmp(value.data(), first.data(), first.size()), 0);
    EXPECT_EQ(std::memcmp(value.data() + first.size(), second.data(),
                          second.size()),
              0);

    // Batch across two more keys.
    std::vector<std::string> keys{std::string(run_id) + "-batch-0",
                                  std::string(run_id) + "-batch-1"};
    std::vector<std::string> values{std::string(4096, 'N'),
                                    std::string(4096, 'O')};
    auto batch_slices = MakeSlices(values);
    auto put_results = writer_->BatchPut(keys, batch_slices, DfsConfig());
    ASSERT_EQ(put_results.size(), keys.size());
    ASSERT_TRUE(put_results[0].has_value());
    ASSERT_TRUE(put_results[1].has_value());

    std::vector<QueryResult> queries;
    for (const auto& batch_key : keys) {
        auto batch_query = QueryDfsOnly(batch_key);
        ASSERT_TRUE(batch_query.has_value());
        queries.push_back(*batch_query);
    }
    std::vector<std::string> output{std::string(4096, '\0'),
                                    std::string(4096, '\0')};
    std::unordered_map<std::string, std::vector<Slice>> batch_read_slices;
    for (size_t i = 0; i < keys.size(); ++i) {
        batch_read_slices[keys[i]] = {{output[i].data(), output[i].size()}};
    }
    auto get_results = writer_->BatchGet(keys, queries, batch_read_slices);
    ASSERT_EQ(get_results.size(), keys.size());
    ASSERT_TRUE(get_results[0].has_value());
    ASSERT_TRUE(get_results[1].has_value());
    EXPECT_EQ(output[0], values[0]);
    EXPECT_EQ(output[1], values[1]);

    // No local recovery ledger, no persistent object index, no scan on startup:
    // that is the point of this mode, and the user has confirmed that losing the
    // cache on restart is acceptable. A runner may delete the three keys it knows
    // in advance (`<RUN_ID>-single`, `-batch-0`, `-batch-1`) once timing is over;
    // an orphan that cannot be confirmed after a failure is an accepted
    // limitation rather than a reason to add a recovery mechanism. Namespace
    // deletion is the intended future route for whole-pool reclamation and is
    // deliberately not implemented or probed here.
}

namespace {

std::vector<size_t> ParseSizes(const char* text) {
    std::vector<size_t> sizes;
    if (text == nullptr) {
        return sizes;
    }
    std::string token;
    std::istringstream in(text);
    while (std::getline(in, token, ',')) {
        if (!token.empty()) {
            sizes.push_back(static_cast<size_t>(std::stoull(token)));
        }
    }
    return sizes;
}

double Percentile(std::vector<double> values, double p) {
    if (values.empty()) {
        return 0.0;
    }
    std::sort(values.begin(), values.end());
    const size_t index = std::min(
        values.size() - 1,
        static_cast<size_t>(p * static_cast<double>(values.size() - 1)));
    return values[index];
}

// One pass over a set of keys that performs the Master directory lookup
// **outside** the timed region and times only the data path. Every item is
// checked: a failed query, a missing DFS descriptor, a descriptor whose
// object_size disagrees with the request, a failed `BatchGet`, or content that
// differs from what was written counts against the pass, and that batch
// contributes neither time nor bytes. A pass is usable only when it had no
// failures and no mismatches at all.
struct DataPathPass {
    std::size_t batches = 0;
    std::size_t items = 0;
    std::uint64_t bytes = 0;
    std::size_t failures = 0;    // query / descriptor / transport failures
    std::size_t mismatches = 0;  // read succeeded but returned other bytes
    double seconds = 0;
    std::vector<double> batch_ms;

    bool usable() const {
        return batches > 0 && failures == 0 && mismatches == 0;
    }
};

DataPathPass RunDataPathPass(Client& reader,
                             const std::vector<std::string>& keys,
                             std::size_t size,
                             const std::vector<std::string>& expected,
                             std::size_t width) {
    DataPathPass pass;
    for (std::size_t begin = 0; begin < keys.size(); begin += width) {
        const std::size_t end = std::min(keys.size(), begin + width);
        std::vector<std::string> batch_keys(keys.begin() + begin,
                                            keys.begin() + end);

        // Untimed: directory lookup.
        auto query_results = reader.BatchQuery(batch_keys);
        if (query_results.size() != batch_keys.size()) {
            pass.failures += batch_keys.size();
            continue;
        }
        std::vector<std::string> output;
        output.reserve(batch_keys.size());
        std::unordered_map<std::string, std::vector<Slice>> read_slices;
        std::vector<QueryResult> dfs_queries;
        dfs_queries.reserve(batch_keys.size());
        bool prepared = true;
        for (std::size_t i = 0; i < batch_keys.size(); ++i) {
            if (!query_results[i].has_value()) {
                ++pass.failures;
                prepared = false;
                continue;
            }
            std::vector<Replica::Descriptor> dfs;
            for (const auto& replica : query_results[i]->replicas) {
                if (replica.is_dfs_replica()) {
                    dfs.push_back(replica);
                }
            }
            bool length_ok = false;
            for (const auto& descriptor : dfs) {
                if (descriptor.get_dfs_descriptor().object_size == size) {
                    length_ok = true;
                    break;
                }
            }
            if (dfs.empty() || !length_ok) {
                ++pass.failures;
                prepared = false;
                continue;
            }
            dfs_queries.emplace_back(std::move(dfs),
                                     query_results[i]->lease_timeout,
                                     query_results[i]->object_checksum);
            output.emplace_back(size, '\0');
            read_slices[batch_keys[i]] = {{output.back().data(), size}};
        }
        if (!prepared) {
            continue;  // no timing for a batch that could not be prepared
        }

        const auto t0 = std::chrono::steady_clock::now();
        auto get_results = reader.BatchGet(batch_keys, dfs_queries, read_slices);
        const auto t1 = std::chrono::steady_clock::now();
        if (get_results.size() != batch_keys.size()) {
            pass.failures += batch_keys.size();
            continue;
        }
        std::size_t batch_failures = 0;
        std::size_t batch_mismatches = 0;
        for (std::size_t i = 0; i < batch_keys.size(); ++i) {
            if (!get_results[i].has_value()) {
                ++batch_failures;
            } else if (output[i] != expected[begin + i]) {
                ++batch_mismatches;
            }
        }
        pass.failures += batch_failures;
        pass.mismatches += batch_mismatches;
        if (batch_failures != 0 || batch_mismatches != 0) {
            continue;  // a partially failed batch contributes no throughput
        }
        const double batch_s =
            std::chrono::duration<double>(t1 - t0).count();
        pass.batch_ms.push_back(batch_s * 1000.0);
        pass.seconds += batch_s;
        pass.bytes += static_cast<std::uint64_t>(batch_keys.size()) * size;
        pass.items += batch_keys.size();
        ++pass.batches;
    }
    return pass;
}

}  // namespace

// Full-stack HOST throughput: Client -> Master -> DFS adapter -> kvclient.
//
// The layered numbers already published (`FINAL-REPORT.md`) stop at the object
// core; this is the layer above it. Two things are deliberate:
//
//  * the read side queries the Master for the DFS descriptor and then reads,
//    both inside the timed region, so the figure is a client request's cost and
//    not the data path's cost with the directory lookup quietly excluded;
//  * the reader is a *separate* Client with its own backend over the same
//    instance and epoch, so a round trip cannot be satisfied by the writer's own
//    cached state.
TEST_F(DfsSyncClientTest, ObjectStorageModeStackThroughput) {
    const char* sdk_config = std::getenv("MOONCAKE_B2_KVCLIENT_CONFIG");
    const char* instance = std::getenv("MOONCAKE_B2_INSTANCE");
    const char* epoch = std::getenv("MOONCAKE_B2_EPOCH");
    const char* run_id = std::getenv("MOONCAKE_B2_RUN_ID");
    if (sdk_config == nullptr || instance == nullptr || epoch == nullptr ||
        run_id == nullptr) {
        GTEST_SKIP() << "stack throughput needs MOONCAKE_B2_KVCLIENT_CONFIG, "
                        "MOONCAKE_B2_INSTANCE, MOONCAKE_B2_EPOCH and "
                        "MOONCAKE_B2_RUN_ID";
    }
    std::vector<size_t> sizes = ParseSizes(std::getenv("MOONCAKE_STACK_SIZES"));
    if (sizes.empty()) {
        sizes = {128 * 1024, 1310720};
    }
    const char* width_text = std::getenv("MOONCAKE_STACK_BATCH");
    const size_t width =
        width_text != nullptr ? static_cast<size_t>(std::stoull(width_text)) : 32;
    const char* phase_bytes_text = std::getenv("MOONCAKE_STACK_PHASE_BYTES");
    const char* rounds_text = std::getenv("MOONCAKE_STACK_ROUNDS");
    const int rounds = rounds_text != nullptr ? std::stoi(rounds_text) : 3;
    ASSERT_GT(width, 0u);
    ASSERT_GT(rounds, 0);

    SetEnv("MOONCAKE_DFS_FS_ADAPTER", "object_storage");
    SetEnv("MOONCAKE_DFS_OBJECT_CACHE_INSTANCE", instance);
    SetEnv("MOONCAKE_DFS_OBJECT_EPOCH", epoch);
    SetEnv("MOONCAKE_DFS_KVCLIENT_SDK_CONFIG", sdk_config);
    SetEnv("MOONCAKE_DFS_KVCLIENT_TIMEOUT_MS", "15000");

    // A larger MEMORY segment for the writer, freed on the way out. This is
    // headroom rather than the binding constraint: the first timed runs here
    // failed with `NO_AVAILABLE_HANDLE` out of the *DFS* allocator, not with a
    // MEMORY allocation error, so the allocator size (see
    // MOONCAKE_STACK_DFS_SHARD_*) is what a round is really bounded by.
    const char* segment_text = std::getenv("MOONCAKE_STACK_SEGMENT_BYTES");
    const size_t stack_segment_bytes =
        segment_text != nullptr
            ? static_cast<size_t>(std::stoull(segment_text))
            : (512u * 1024u * 1024u);
    void* stack_segment = allocate_buffer_allocator_memory(stack_segment_bytes);
    ASSERT_NE(stack_segment, nullptr);
    ASSERT_TRUE(
        writer_->MountSegment(stack_segment, stack_segment_bytes, "tcp")
            .has_value());

    FileStorageConfig file_config;
    file_config.storage_backend_type = StorageBackendType::kDistributed;
    file_config.storage_filepath = root_;

    auto created = CreateStorageBackend(file_config);
    ASSERT_TRUE(created.has_value());
    auto object_backend =
        std::dynamic_pointer_cast<DistributedStorageBackend>(*created);
    ASSERT_NE(object_backend, nullptr);
    ASSERT_TRUE(object_backend->UsesObjectStorage());
    ASSERT_TRUE(object_backend->Init().has_value());
    backend_ = object_backend;
    writer_->SetDfsStorageBackend(backend_);

    // The reader is a second backend over the same instance and epoch, driven by
    // a different Client: the deployment's "single writer, multiple readers".
    auto reader_created = CreateStorageBackend(file_config);
    ASSERT_TRUE(reader_created.has_value());
    auto reader_backend =
        std::dynamic_pointer_cast<DistributedStorageBackend>(*reader_created);
    ASSERT_NE(reader_backend, nullptr);
    ASSERT_TRUE(reader_backend->UsesObjectStorage());
    ASSERT_TRUE(reader_backend->Init().has_value());
    provider_->SetDfsStorageBackend(reader_backend);

    const char* min_round_text =
        std::getenv("MOONCAKE_STACK_MIN_ROUND_SECONDS");
    const double min_round_s =
        min_round_text != nullptr ? std::stod(min_round_text) : 30.0;
    const char* round_bytes_text = std::getenv("MOONCAKE_STACK_ROUND_BYTES");
    const std::uint64_t round_byte_budget =
        round_bytes_text != nullptr
            ? static_cast<std::uint64_t>(std::stoull(round_bytes_text))
            : 0;
    const size_t phase_budget =
        phase_bytes_text != nullptr
            ? static_cast<size_t>(std::stoull(phase_bytes_text))
            : (16u * 1024u * 1024u);

    // What actually bounds a phase is the *DFS allocator*, not the MEMORY
    // segment: a phase that outgrows the DFS shards fails with
    // `NO_AVAILABLE_HANDLE` inside the Master's `BatchPutStart`. The allocator
    // has no eviction here, so its capacity is cumulative over the whole run.
    // A round therefore keeps adding bounded fresh-key phases until it has done
    // `min_round_s` of timed work, and reports the aggregate; one bigger phase
    // would hit the same allocator ceiling instead of measuring more.
    struct PhaseResult {
        std::uint64_t objects = 0;
        std::uint64_t write_bytes = 0;
        std::uint64_t read_bytes = 0;
        double write_s = 0;
        double read_s = 0;
        std::size_t mismatches = 0;
        // Second read pass with the directory lookup outside the timed window:
        // the data-path-only figure, reported separately from full-stack read.
        std::uint64_t datapath_bytes = 0;
        std::size_t datapath_items = 0;
        std::size_t datapath_failures = 0;
        std::size_t datapath_mismatches = 0;
        std::size_t cleanup_query_failures = 0;
        double datapath_s = 0;
        std::vector<double> datapath_batch_ms;
        // Off-timing precise deletion of this phase's own keys.
        std::size_t keys = 0;
        std::size_t removed = 0;
        std::size_t physically_deleted = 0;
        std::size_t still_present = 0;
        int remove_error = 0;
        int physical_error = 0;
        // A phase that fails must end its round, not be retried: the failure
        // that matters here is the allocator being full, which no retry fixes.
        // Without this flag the round loop keeps adding zero-length phases and
        // spins (one aborted run produced 131,288 failed phases before it was
        // stopped).
        bool ok = true;
        std::string failure;
        std::vector<double> write_batch_ms;
        std::vector<double> read_batch_ms;
    };

    auto run_phase = [&](size_t size, const std::string& tag,
                         PhaseResult& out) {
        const size_t objects =
            std::max<size_t>(width, std::max<size_t>(1, phase_budget / size));
        std::vector<std::string> values(objects, std::string(size, 's'));
        std::vector<std::string> keys;
        keys.reserve(objects);
        for (size_t i = 0; i < objects; ++i) {
            keys.push_back(tag + "-" + std::to_string(i));
        }

        std::uint64_t written_bytes = 0;
        const auto write_start = std::chrono::steady_clock::now();
        for (size_t begin = 0; begin < objects; begin += width) {
            const size_t end = std::min(objects, begin + width);
            std::vector<std::string> batch_keys(keys.begin() + begin,
                                                keys.begin() + end);
            std::vector<std::string> batch_values(values.begin() + begin,
                                                  values.begin() + end);
            auto slices = MakeSlices(batch_values);
            const auto t0 = std::chrono::steady_clock::now();
            auto results = writer_->BatchPut(batch_keys, slices, DfsConfig());
            const auto t1 = std::chrono::steady_clock::now();
            if (results.size() != batch_keys.size()) {
                out.ok = false;
                out.failure = "BatchPut returned " +
                              std::to_string(results.size()) + " results for " +
                              std::to_string(batch_keys.size()) + " keys";
                return;
            }
            out.write_batch_ms.push_back(
                std::chrono::duration<double, std::milli>(t1 - t0).count());
            for (const auto& result : results) {
                if (!result.has_value()) {
                    out.ok = false;
                    out.failure = "write failed for phase " + tag;
                    out.write_s = std::chrono::duration<double>(
                                      t1 - write_start)
                                      .count();
                    ADD_FAILURE() << out.failure;
                    return;
                }
            }
            written_bytes += static_cast<std::uint64_t>(end - begin) * size;
        }
        const auto write_end = std::chrono::steady_clock::now();

        // Read: directory query and data read both inside the timed region,
        // through the *reader* client.
        std::uint64_t read_bytes = 0;
        size_t mismatches = 0;
        const auto read_start = std::chrono::steady_clock::now();
        for (size_t begin = 0; begin < objects; begin += width) {
            const size_t end = std::min(objects, begin + width);
            std::vector<std::string> batch_keys(keys.begin() + begin,
                                                keys.begin() + end);
            const auto t0 = std::chrono::steady_clock::now();
            auto query_results = provider_->BatchQuery(batch_keys);
            if (query_results.size() != batch_keys.size()) {
                out.ok = false;
                out.failure = "BatchQuery returned " +
                              std::to_string(query_results.size()) + " results for " +
                              std::to_string(batch_keys.size()) + " keys";
                return;
            }
            std::vector<std::string> output;
            output.reserve(batch_keys.size());
            std::unordered_map<std::string, std::vector<Slice>> read_slices;
            std::vector<QueryResult> dfs_queries;
            dfs_queries.reserve(batch_keys.size());
            for (size_t i = 0; i < batch_keys.size(); ++i) {
                if (!query_results[i].has_value()) {
                    out.ok = false;
                    out.failure = "query failed for " + batch_keys[i];
                    ADD_FAILURE() << out.failure;
                    return;
                }
                std::vector<Replica::Descriptor> dfs;
                for (const auto& replica : query_results[i]->replicas) {
                    if (replica.is_dfs_replica()) {
                        dfs.push_back(replica);
                    }
                }
                // Excluding MEMORY is the whole point: without it the read
                // may never reach the object path at all.
                if (dfs.empty()) {
                    out.ok = false;
                    out.failure = "no DFS replica for " + batch_keys[i];
                    ADD_FAILURE() << out.failure;
                    return;
                }
                dfs_queries.emplace_back(std::move(dfs),
                                         query_results[i]->lease_timeout,
                                         query_results[i]->object_checksum);
                output.emplace_back(size, '\0');
                read_slices[batch_keys[i]] = {{output.back().data(), size}};
            }
            auto get_results =
                provider_->BatchGet(batch_keys, dfs_queries, read_slices);
            const auto t1 = std::chrono::steady_clock::now();
            if (get_results.size() != batch_keys.size()) {
                out.ok = false;
                out.failure = "BatchGet returned " +
                              std::to_string(get_results.size()) + " results for " +
                              std::to_string(batch_keys.size()) + " keys";
                return;
            }
            out.read_batch_ms.push_back(
                std::chrono::duration<double, std::milli>(t1 - t0).count());
            ASSERT_EQ(get_results.size(), batch_keys.size());
            for (size_t i = 0; i < batch_keys.size(); ++i) {
                if (!get_results[i].has_value()) {
                    out.ok = false;
                    out.failure = "read failed for " + batch_keys[i];
                    ADD_FAILURE() << out.failure;
                    return;
                }
                if (output[i] != values[begin + i]) {
                    ++mismatches;
                }
            }
            read_bytes +=
                static_cast<std::uint64_t>(batch_keys.size()) * size;
        }
        const auto read_end = std::chrono::steady_clock::now();

        // Second read pass: same bytes, but every `BatchQuery` is issued
        // *outside* the timed region, so this measures the data path alone.
        // Strictly checked (see RunDataPathPass): only a failure-free pass
        // contributes numbers, and an incomplete pass fails the round.
        // `datapath_s` is the sum of the per-batch `BatchGet` timings and is
        // not comparable one-for-one with `read_s` (which also times the
        // lookups, the destination construction and the byte comparison), so
        // the gap between them is reported as an observation, never as an
        // isolated share of the directory lookup.
        const DataPathPass pass =
            RunDataPathPass(*provider_, keys, size, values, width);
        out.datapath_s = pass.seconds;
        out.datapath_bytes = pass.bytes;
        out.datapath_items = pass.items;
        out.datapath_failures = pass.failures;
        out.datapath_mismatches = pass.mismatches;
        out.datapath_batch_ms = pass.batch_ms;
        if (!pass.usable() || pass.items != objects) {
            // Do not return: the off-timing cleanup below still has to run.
            out.ok = false;
            out.failure = "data-path pass is not usable (items=" +
                          std::to_string(pass.items) + "/" +
                          std::to_string(objects) + ", failures=" +
                          std::to_string(pass.failures) + ", mismatches=" +
                          std::to_string(pass.mismatches) + ")";
        }

        out.objects = objects;
        out.write_bytes = written_bytes;
        out.read_bytes = read_bytes;
        out.mismatches = mismatches;
        out.write_s =
            std::chrono::duration<double>(write_end - write_start).count();
        out.read_s =
            std::chrono::duration<double>(read_end - read_start).count();

        // Off-timing precise removal of the keys this phase created: both timed
        // regions have closed, so this is not part of any throughput figure.
        // Removal is metadata-level on this path -- the Master frees the DFS
        // allocator handle -- and whether the backend's bytes actually go away
        // is checked separately and reported, not assumed. No ledger, no scan,
        // no namespace deletion, no protocol change: the keys are the ones this
        // phase holds in memory.
        // force=true: the reader's own BatchQuery left a lease on each key, so
        // the lease check would refuse a removal the test itself owns. These
        // keys belong to this phase and nobody else is reading them.
        std::vector<ObjectKey> remove_keys(keys.begin(), keys.end());
        auto remove_results = writer_->BatchRemove(remove_keys,
                                                   /*force=*/true);
        std::size_t removed = 0;
        int first_remove_error = 0;
        std::size_t physically_deleted = 0;
        int first_physical_error = 0;
        if (remove_results.size() == keys.size()) {
            for (std::size_t i = 0; i < remove_results.size(); ++i) {
                if (!remove_results[i].has_value()) {
                    if (first_remove_error == 0) {
                        first_remove_error =
                            static_cast<int>(remove_results[i].error());
                    }
                    continue;
                }
                ++removed;
                // Production Remove()/BatchRemove() deliberately do not touch
                // the object store (see DistributedStorageBackend::RemoveObject),
                // so the test drops the physical object itself through that
                // explicit entry and counts both halves separately.
                if (backend_ != nullptr && backend_->UsesObjectStorage()) {
                    auto physical = backend_->RemoveObject(keys[i]);
                    if (physical.has_value()) {
                        ++physically_deleted;
                    } else if (first_physical_error == 0) {
                        first_physical_error =
                            static_cast<int>(physical.error());
                    }
                }
            }
        } else {
            // A short/long response must not be read as "everything removed".
            first_remove_error = static_cast<int>(ErrorCode::INTERNAL_ERROR);
        }
        out.remove_error = first_remove_error;
        out.physical_error = first_physical_error;
        out.physically_deleted = physically_deleted;

        std::size_t still_present = 0;
        std::size_t cleanup_query_failures = 0;
        if (remove_results.size() == keys.size()) {
            auto after_remove = writer_->BatchQuery(keys);
            if (after_remove.size() == keys.size()) {
                for (const auto& query : after_remove) {
                    if (query.has_value()) {
                        ++still_present;
                    } else if (query.error() != ErrorCode::OBJECT_NOT_FOUND) {
                        // "not found" is the expected answer; anything else
                        // means absence was not actually established.
                        ++cleanup_query_failures;
                    }
                }
            } else {
                cleanup_query_failures = keys.size();
            }
        } else {
            cleanup_query_failures = keys.size();
        }
        out.keys = keys.size();
        out.removed = removed;
        out.still_present = still_present;
        out.cleanup_query_failures = cleanup_query_failures;
        // Fault injection used only by the failure-gate regression: a round in
        // which a phase reports failure must print STACK_FAIL *and* make gtest
        // non-zero. Unset in every measured run.
        const char* inject_suffix =
            std::getenv("MOONCAKE_STACK_INJECT_FAIL_SUFFIX");
        if (inject_suffix != nullptr && std::strlen(inject_suffix) > 0 &&
            tag.size() >= std::strlen(inject_suffix) &&
            tag.compare(tag.size() - std::strlen(inject_suffix),
                        std::strlen(inject_suffix), inject_suffix) == 0) {
            out.ok = false;
            out.failure = "injected failure for the failure-gate regression";
        }

        if (out.ok && (removed != keys.size() ||
                       (backend_ != nullptr && backend_->UsesObjectStorage() &&
                        physically_deleted != keys.size()) ||
                       still_present != 0 || cleanup_query_failures != 0)) {
            out.ok = false;
            out.failure = "cleanup incomplete (removed=" +
                          std::to_string(removed) + "/" +
                          std::to_string(keys.size()) +
                          ", physically_deleted=" +
                          std::to_string(physically_deleted) +
                          ", still_present=" + std::to_string(still_present) +
                          ", query_failures=" +
                          std::to_string(cleanup_query_failures) + ")";
        }
    };

    const double mib = 1024.0 * 1024.0;
    for (size_t size : sizes) {
        // Warm-up: one phase whose numbers are discarded, so every reported
        // round is measured on a stack that has already served traffic.
        {
            PhaseResult warm;
            run_phase(size,
                      std::string(run_id) + "-" + std::to_string(size) + "-warm",
                      warm);
            std::printf(
                "STACK_WARMUP {\"size\":%zu,\"objects\":%llu,\"write_s\":%.3f,"
                "\"read_s\":%.3f,\"mismatches\":%zu}\n",
                size, static_cast<unsigned long long>(warm.objects),
                warm.write_s, warm.read_s, warm.mismatches);
            ASSERT_TRUE(warm.ok) << warm.failure;
            ASSERT_EQ(warm.mismatches, 0u);
            ASSERT_EQ(warm.datapath_failures, 0u) << "warm-up data path failed";
            ASSERT_EQ(warm.datapath_mismatches, 0u);
            ASSERT_EQ(warm.physically_deleted, warm.keys);
            ASSERT_EQ(warm.still_present, 0u);
        }

        for (int round = 0; round < rounds; ++round) {
            PhaseResult total;
            int phases = 0;
            for (;;) {
                PhaseResult phase;
                run_phase(size,
                          std::string(run_id) + "-" + std::to_string(size) +
                              "-r" + std::to_string(round) + "-p" +
                              std::to_string(phases),
                          phase);
                total.objects += phase.objects;
                total.write_bytes += phase.write_bytes;
                total.read_bytes += phase.read_bytes;
                total.write_s += phase.write_s;
                total.read_s += phase.read_s;
                total.mismatches += phase.mismatches;
                total.datapath_bytes += phase.datapath_bytes;
                total.datapath_s += phase.datapath_s;
                total.datapath_batch_ms.insert(total.datapath_batch_ms.end(),
                                               phase.datapath_batch_ms.begin(),
                                               phase.datapath_batch_ms.end());
                total.keys += phase.keys;
                total.removed += phase.removed;
                total.physically_deleted += phase.physically_deleted;
                total.datapath_items += phase.datapath_items;
                total.datapath_failures += phase.datapath_failures;
                total.datapath_mismatches += phase.datapath_mismatches;
                total.cleanup_query_failures += phase.cleanup_query_failures;
                if (total.physical_error == 0) {
                    total.physical_error = phase.physical_error;
                }
                total.still_present += phase.still_present;
                if (total.remove_error == 0) {
                    total.remove_error = phase.remove_error;
                }
                total.write_batch_ms.insert(total.write_batch_ms.end(),
                                            phase.write_batch_ms.begin(),
                                            phase.write_batch_ms.end());
                total.read_batch_ms.insert(total.read_batch_ms.end(),
                                           phase.read_batch_ms.begin(),
                                           phase.read_batch_ms.end());
                ++phases;
                if (!phase.ok) {
                    total.ok = false;
                    total.failure = phase.failure;
                    break;
                }
                const double done_s = total.write_s + total.read_s;
                if (done_s >= min_round_s) {
                    break;
                }
                if (round_byte_budget != 0 &&
                    total.write_bytes >= round_byte_budget) {
                    break;
                }
            }
            if (!total.ok) {
                // A failed round is reported as a failure, with the bytes and
                // phases it did complete, and never as a throughput figure.
                std::printf(
                    "STACK_FAIL {\"run_id\":\"%s\",\"size\":%zu,"
                    "\"round\":%d,\"batch\":%zu,\"phases\":%d,"
                    "\"objects\":%llu,\"write_bytes\":%llu,"
                    "\"write_s\":%.3f,\"failure\":\"%s\"}\n",
                    run_id, size, round, width, phases,
                    static_cast<unsigned long long>(total.objects),
                    static_cast<unsigned long long>(total.write_bytes),
                    total.write_s, total.failure.c_str());
                // A round that failed must make the process fail too: leaving
                // it to the runner means a direct gtest run would exit 0 on a
                // broken round.
                ADD_FAILURE() << "round failed: " << total.failure;
                break;
            }

            std::printf(
                "STACK {\"run_id\":\"%s\",\"size\":%zu,\"round\":%d,"
                "\"batch\":%zu,\"phases\":%d,\"objects\":%llu,"
                "\"write_bytes\":%llu,\"read_bytes\":%llu,"
                "\"write_s\":%.3f,\"read_s\":%.3f,"
                "\"write_mib_s\":%.1f,\"read_mib_s\":%.1f,"
                "\"write_ops_s\":%.1f,\"read_ops_s\":%.1f,"
                "\"write_p50_ms\":%.3f,\"write_p95_ms\":%.3f,"
                "\"write_p99_ms\":%.3f,\"read_p50_ms\":%.3f,"
                "\"read_p95_ms\":%.3f,\"read_p99_ms\":%.3f,"
                "\"mismatches\":%llu,\"min_round_s\":%.1f,"
                "\"keys\":%llu,\"removed\":%llu,\"physically_deleted\":%llu,"
                "\"still_present\":%llu,"
                "\"datapath_mib_s\":%.1f,\"datapath_s\":%.3f,"
                "\"datapath_p50_ms\":%.3f,\"datapath_p95_ms\":%.3f,"
                "\"datapath_p99_ms\":%.3f,\"datapath_items\":%llu,"
                "\"datapath_failures\":%llu,\"datapath_mismatches\":%llu,"
                "\"cleanup_query_failures\":%llu}\n",
                run_id, size, round, width, phases,
                static_cast<unsigned long long>(total.objects),
                static_cast<unsigned long long>(total.write_bytes),
                static_cast<unsigned long long>(total.read_bytes),
                total.write_s, total.read_s,
                static_cast<double>(total.write_bytes) / mib / total.write_s,
                static_cast<double>(total.read_bytes) / mib / total.read_s,
                static_cast<double>(total.objects) / total.write_s,
                static_cast<double>(total.objects) / total.read_s,
                Percentile(total.write_batch_ms, 0.50),
                Percentile(total.write_batch_ms, 0.95),
                Percentile(total.write_batch_ms, 0.99),
                Percentile(total.read_batch_ms, 0.50),
                Percentile(total.read_batch_ms, 0.95),
                Percentile(total.read_batch_ms, 0.99),
                static_cast<unsigned long long>(total.mismatches), min_round_s,
                static_cast<unsigned long long>(total.keys),
                static_cast<unsigned long long>(total.removed),
                static_cast<unsigned long long>(total.physically_deleted),
                static_cast<unsigned long long>(total.still_present),
                total.datapath_s > 0
                    ? static_cast<double>(total.datapath_bytes) / mib /
                          total.datapath_s
                    : 0.0,
                total.datapath_s,
                Percentile(total.datapath_batch_ms, 0.50),
                Percentile(total.datapath_batch_ms, 0.95),
                Percentile(total.datapath_batch_ms, 0.99),
                static_cast<unsigned long long>(total.datapath_items),
                static_cast<unsigned long long>(total.datapath_failures),
                static_cast<unsigned long long>(total.datapath_mismatches),
                static_cast<unsigned long long>(total.cleanup_query_failures));
            std::printf(
                "STACK_CLEANUP {\"run_id\":\"%s\",\"size\":%zu,\"round\":%d,"
                "\"keys\":%llu,\"removed\":%llu,\"physically_deleted\":%llu,"
                "\"still_present\":%llu,\"cleanup_query_failures\":%llu,"
                "\"first_remove_error\":%d,\"first_physical_error\":%d}\n",
                run_id, size, round,
                static_cast<unsigned long long>(total.keys),
                static_cast<unsigned long long>(total.removed),
                static_cast<unsigned long long>(total.physically_deleted),
                static_cast<unsigned long long>(total.still_present),
                static_cast<unsigned long long>(total.cleanup_query_failures),
                total.remove_error, total.physical_error);
            EXPECT_EQ(total.mismatches, 0u);
            EXPECT_EQ(total.datapath_failures, 0u) << "data-path read failed";
            EXPECT_EQ(total.datapath_mismatches, 0u) << "data-path read mismatch";
            EXPECT_EQ(total.datapath_items, total.objects);
            EXPECT_EQ(total.removed, total.keys);
            EXPECT_EQ(total.physically_deleted, total.keys);
            EXPECT_EQ(total.still_present, 0u);
            EXPECT_EQ(total.cleanup_query_failures, 0u);
        }
    }
    (void)writer_->UnmountSegment(stack_segment, stack_segment_bytes);
}

// The data-path pass must refuse what it could not fully validate. A missing
// key, and content that differs from what was written, must both leave the pass
// unusable and must contribute neither bytes nor items -- otherwise a broken
// read would be averaged into a throughput number. This runs on the fixture's
// default POSIX DFS backend, so it needs no kvclient/RDMA and always executes.
TEST_F(DfsSyncClientTest, ObjectStorageModeDataPathRefusesFailedReads) {
    provider_->SetDfsStorageBackend(backend_);

    const std::size_t size = 4096;
    const std::string payload(size, 'p');
    const std::string other(size, 'q');  // deliberately different bytes

    std::vector<std::string> keys = {"dpgate-present"};
    std::vector<std::string> values = {payload};
    auto slices = MakeSlices(values);
    auto put = writer_->BatchPut(keys, slices, DfsConfig());
    ASSERT_EQ(put.size(), 1u);
    ASSERT_TRUE(put.front().has_value()) << "could not write the control object";

    // Positive control: the same key, fully checked, is usable.
    auto ok = RunDataPathPass(*provider_, keys, size, values, 4);
    EXPECT_TRUE(ok.usable());
    EXPECT_EQ(ok.items, 1u);
    EXPECT_EQ(ok.bytes, size);
    EXPECT_EQ(ok.failures, 0u);
    EXPECT_EQ(ok.mismatches, 0u);

    // A key that was never written must not be counted.
    std::vector<std::string> with_missing = {keys[0], "dpgate-absent"};
    std::vector<std::string> expected_pair = {payload, payload};
    auto missing =
        RunDataPathPass(*provider_, with_missing, size, expected_pair, 4);
    EXPECT_FALSE(missing.usable()) << "a missing key was counted as a read";
    EXPECT_GE(missing.failures, 1u);
    EXPECT_EQ(missing.items, 0u);
    EXPECT_EQ(missing.bytes, 0u);

    // Wrong expected content must be a mismatch, not a success.
    std::vector<std::string> wrong = {other};
    auto mismatch = RunDataPathPass(*provider_, keys, size, wrong, 4);
    EXPECT_FALSE(mismatch.usable()) << "different bytes were counted as a read";
    EXPECT_EQ(mismatch.mismatches, 1u);
    EXPECT_EQ(mismatch.failures, 0u);
    EXPECT_EQ(mismatch.items, 0u);
    EXPECT_EQ(mismatch.bytes, 0u);

    std::vector<ObjectKey> remove_keys(keys.begin(), keys.end());
    auto removed = writer_->BatchRemove(remove_keys, /*force=*/true);
    ASSERT_EQ(removed.size(), remove_keys.size());
    EXPECT_TRUE(removed.front().has_value());
}
#endif  // MOONCAKE_WITH_KVCLIENT_OBJECT

}  // namespace mooncake::test
