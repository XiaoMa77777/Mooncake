#include "storage/distributed/object_storage_adapter.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <filesystem>
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <unistd.h>

#include "storage/distributed/distributed_storage_backend.h"

namespace mooncake {
namespace {

class FakeObjectStorageAdapter : public ObjectStorageAdapter {
   public:
    std::map<std::string, std::string> objects;
    std::unordered_set<std::string> fail_keys;
    int init_calls = 0;
    int putv_calls = 0;
    int get_calls = 0;
    bool initialized = false;

    tl::expected<void, ErrorCode> Put(const std::string& logical_key,
                                      std::span<const char> data) override {
        objects[logical_key] = std::string(data.begin(), data.end());
        return {};
    }

    tl::expected<void, ErrorCode> PutV(const std::string& logical_key,
                                       const iovec* iov, int iovcnt) override {
        ++putv_calls;
        if (fail_keys.contains(logical_key)) {
            return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }

        std::string buffer;
        for (int i = 0; i < iovcnt; ++i) {
            buffer.append(static_cast<const char*>(iov[i].iov_base),
                          iov[i].iov_len);
        }
        objects[logical_key] = std::move(buffer);
        return {};
    }

    tl::expected<size_t, ErrorCode> Get(const std::string& logical_key,
                                        void* buf, size_t len) override {
        ++get_calls;
        auto it = objects.find(logical_key);
        if (it == objects.end()) {
            return tl::make_unexpected(ErrorCode::FILE_NOT_FOUND);
        }

        // Whole object or nothing. The truncated alternative -- returning
        // min(len, actual) -- reads as success while delivering part of the
        // object, which is the failure mode the adapter contract now names.
        if (it->second.size() > len) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (!it->second.empty()) {
            std::memcpy(buf, it->second.data(), it->second.size());
        }
        return it->second.size();
    }

    tl::expected<bool, ErrorCode> Exists(
        const std::string& logical_key) override {
        return objects.contains(logical_key);
    }

    tl::expected<void, ErrorCode> Delete(
        const std::string& logical_key) override {
        objects.erase(logical_key);
        return {};
    }

    tl::expected<std::vector<KeyInfo>, ErrorCode> ListKeys() override {
        std::vector<KeyInfo> result;
        result.reserve(objects.size());
        for (const auto& [key, value] : objects) {
            result.push_back({key, value.size()});
        }
        return result;
    }

    tl::expected<void, ErrorCode> Init() override {
        ++init_calls;
        initialized = true;
        return {};
    }

    const char* GetName() const override { return "fake-object-storage"; }
};

class FakeFileSystemAdapter : public FileSystemAdapter {
   public:
    bool initialized = false;

    tl::expected<size_t, ErrorCode> WriteFile(
        const std::string&, std::span<const char> data) override {
        return data.size();
    }

    tl::expected<size_t, ErrorCode> ReadFile(const std::string&, void*,
                                             size_t) override {
        return 0;
    }

    tl::expected<size_t, ErrorCode> VectorWriteFile(const std::string&,
                                                    const iovec* iov,
                                                    int iovcnt,
                                                    off_t) override {
        size_t total = 0;
        for (int i = 0; i < iovcnt; ++i) total += iov[i].iov_len;
        return total;
    }

    tl::expected<size_t, ErrorCode> VectorReadFile(const std::string&,
                                                   const iovec*, int,
                                                   off_t) override {
        return 0;
    }

    tl::expected<void, ErrorCode> DeleteFile(const std::string&) override {
        return {};
    }

    tl::expected<bool, ErrorCode> FileExists(const std::string&) override {
        return false;
    }

    tl::expected<std::vector<std::string>, ErrorCode> ListFiles(
        const std::string&) override {
        return std::vector<std::string>{};
    }

    tl::expected<void, ErrorCode> Init(const std::string&) override {
        initialized = true;
        return {};
    }

    tl::expected<void, ErrorCode> Shutdown() override {
        initialized = false;
        return {};
    }

    const char* GetName() const override { return "fake-filesystem"; }
};

class ObjectStorageAdapterTest : public ::testing::Test {
   protected:
    void SetUp() override {
        root_dir_ = std::filesystem::temp_directory_path() /
                    ("mooncake_object_storage_adapter_test_" +
                     std::to_string(getpid()));
        std::error_code ec;
        std::filesystem::remove_all(root_dir_, ec);
    }

    void TearDown() override {
        std::error_code ec;
        std::filesystem::remove_all(root_dir_, ec);
    }

    std::unique_ptr<DistributedStorageBackend> MakeObjectStorageBackend(
        FakeObjectStorageAdapter*& adapter,
        FileStorageConfig file_config = {}) const {
        DistributedStorageConfig distributed_config;
        distributed_config.fsdir = root_dir_.string();
        auto owned_adapter = std::make_unique<FakeObjectStorageAdapter>();
        adapter = owned_adapter.get();
        return std::make_unique<DistributedStorageBackend>(
            file_config, distributed_config, nullptr, std::move(owned_adapter));
    }

    std::filesystem::path root_dir_;
};

TEST_F(ObjectStorageAdapterTest, ObjectStorageModeInitSkipsDirectories) {
    FakeObjectStorageAdapter* adapter = nullptr;
    auto backend = MakeObjectStorageBackend(adapter);

    EXPECT_EQ(backend->GetStorageMode(),
              DistributedStorageMode::kObjectStorage);
    EXPECT_TRUE(backend->UsesObjectStorage());
    ASSERT_TRUE(backend->Init());
    EXPECT_TRUE(adapter->initialized);
    EXPECT_EQ(adapter->init_calls, 1);
    EXPECT_FALSE(std::filesystem::exists(root_dir_));

    ASSERT_TRUE(backend->Init());
    EXPECT_EQ(adapter->init_calls, 1);
}

TEST_F(ObjectStorageAdapterTest, ObjectStorageModeServesDfsRequests) {
    FakeObjectStorageAdapter* adapter = nullptr;
    auto backend = MakeObjectStorageBackend(adapter);
    ASSERT_TRUE(backend->Init());

    // Object mode used to refuse these outright. It serves them now; what it
    // still refuses is a request with nothing to address -- no key, no declared
    // object size -- and it refuses it as malformed, not as unsupported. The
    // distinction matters: NOT_SUPPORTED would say "this mode cannot do DFS
    // reads", which is no longer true.
    auto write_results = backend->BatchWrite({DfsWriteRequest{}});
    ASSERT_EQ(write_results.size(), 1);
    ASSERT_FALSE(write_results.front());
    EXPECT_EQ(write_results.front().error(), ErrorCode::INVALID_PARAMS);

    auto read_results = backend->BatchRead({DfsReadRequest{}});
    ASSERT_EQ(read_results.size(), 1);
    ASSERT_FALSE(read_results.front());
    EXPECT_EQ(read_results.front().error(), ErrorCode::INVALID_PARAMS);

    // A well-formed request round-trips through the adapter.
    std::string payload = "object-payload";
    DfsWriteRequest write_request;
    write_request.key = "served";
    write_request.descriptor.object_size = payload.size();
    write_request.slices = {{payload.data(), payload.size()}};
    auto write = backend->BatchWrite({write_request});
    ASSERT_EQ(write.size(), 1);
    EXPECT_TRUE(write.front().has_value());
    EXPECT_EQ(adapter->objects.at("served"), payload);

    std::vector<char> back(payload.size(), '\0');
    DfsReadRequest read_request;
    read_request.key = "served";
    read_request.descriptor.object_size = payload.size();
    read_request.slices = {{back.data(), back.size()}};
    auto read = backend->BatchRead({read_request});
    ASSERT_EQ(read.size(), 1);
    EXPECT_TRUE(read.front().has_value());
    EXPECT_EQ(std::string(back.begin(), back.end()), payload);
}

TEST_F(ObjectStorageAdapterTest, BatchOffloadWritesMultiSliceObjects) {
    FakeObjectStorageAdapter* adapter = nullptr;
    auto backend = MakeObjectStorageBackend(adapter);
    ASSERT_TRUE(backend->Init());

    std::string first_a = "hello";
    std::string first_b = "-world";
    std::string second = "payload";
    std::unordered_map<std::string, std::vector<Slice>> batch{
        {"first",
         {{first_a.data(), first_a.size()}, {first_b.data(), first_b.size()}}},
        {"second", {{second.data(), second.size()}}},
    };
    std::vector<std::string> completed_keys;
    std::vector<StorageObjectMetadata> completed_metadata;

    auto result = backend->BatchOffload(
        batch, [&](const std::vector<std::string>& keys,
                   std::vector<StorageObjectMetadata>& metadata) {
            completed_keys = keys;
            completed_metadata = metadata;
            return ErrorCode::OK;
        });

    ASSERT_TRUE(result);
    EXPECT_EQ(*result, 2);
    EXPECT_EQ(adapter->putv_calls, 2);
    EXPECT_EQ(adapter->objects.at("first"), "hello-world");
    EXPECT_EQ(adapter->objects.at("second"), "payload");
    EXPECT_EQ(completed_keys.size(), 2);
    ASSERT_EQ(completed_metadata.size(), 2);

    std::map<std::string, int64_t> sizes;
    for (size_t i = 0; i < completed_keys.size(); ++i) {
        sizes[completed_keys[i]] = completed_metadata[i].data_size;
    }
    EXPECT_EQ(sizes.at("first"), 11);
    EXPECT_EQ(sizes.at("second"), 7);
}

TEST_F(ObjectStorageAdapterTest, PreservesOpaqueLogicalKeys) {
    FakeObjectStorageAdapter* adapter = nullptr;
    auto backend = MakeObjectStorageBackend(adapter);
    ASSERT_TRUE(backend->Init());

    std::string logical_key = "tenant";
    logical_key.push_back('\0');
    logical_key += "a/b%\\";
    std::string payload = "payload";
    std::unordered_map<std::string, std::vector<Slice>> batch{
        {logical_key, {{payload.data(), payload.size()}}},
    };
    std::vector<std::string> completed_keys;

    auto offload_result =
        backend->BatchOffload(batch, [&](const std::vector<std::string>& keys,
                                         std::vector<StorageObjectMetadata>&) {
            completed_keys = keys;
            return ErrorCode::OK;
        });

    ASSERT_TRUE(offload_result);
    EXPECT_EQ(*offload_result, 1);
    EXPECT_EQ(completed_keys, (std::vector<std::string>{logical_key}));
    EXPECT_TRUE(adapter->objects.contains(logical_key));

    auto exists_result = backend->IsExist(logical_key);
    ASSERT_TRUE(exists_result);
    EXPECT_TRUE(*exists_result);

    std::string output(payload.size(), '\0');
    std::unordered_map<std::string, Slice> load_slices{
        {logical_key, {output.data(), output.size()}},
    };
    ASSERT_TRUE(backend->BatchLoad(load_slices));
    EXPECT_EQ(output, payload);

    std::vector<std::string> scanned_keys;
    std::vector<StorageObjectMetadata> scanned_metadata;
    auto scan_result =
        backend->ScanMeta([&](const std::vector<std::string>& keys,
                              std::vector<StorageObjectMetadata>& metadata) {
            scanned_keys.insert(scanned_keys.end(), keys.begin(), keys.end());
            scanned_metadata.insert(scanned_metadata.end(), metadata.begin(),
                                    metadata.end());
            return ErrorCode::OK;
        });

    ASSERT_TRUE(scan_result);
    EXPECT_EQ(scanned_keys, (std::vector<std::string>{logical_key}));
    ASSERT_EQ(scanned_metadata.size(), 1);
    EXPECT_EQ(scanned_metadata.front().key_size,
              static_cast<int64_t>(logical_key.size()));
}

TEST_F(ObjectStorageAdapterTest, BatchOffloadAllowsPartialFailure) {
    FakeObjectStorageAdapter* adapter = nullptr;
    auto backend = MakeObjectStorageBackend(adapter);
    ASSERT_TRUE(backend->Init());
    adapter->fail_keys.insert("bad");

    std::string good = "good-data";
    std::string bad = "bad-data";
    std::unordered_map<std::string, std::vector<Slice>> batch{
        {"good", {{good.data(), good.size()}}},
        {"bad", {{bad.data(), bad.size()}}},
    };
    std::vector<std::string> completed_keys;

    auto result =
        backend->BatchOffload(batch, [&](const std::vector<std::string>& keys,
                                         std::vector<StorageObjectMetadata>&) {
            completed_keys = keys;
            return ErrorCode::OK;
        });

    ASSERT_TRUE(result);
    EXPECT_EQ(*result, 1);
    ASSERT_EQ(completed_keys.size(), 1);
    EXPECT_EQ(completed_keys.front(), "good");
    EXPECT_TRUE(adapter->objects.contains("good"));
    EXPECT_FALSE(adapter->objects.contains("bad"));
}

TEST_F(ObjectStorageAdapterTest, BatchOffloadAllowsNullCompleteHandler) {
    FakeObjectStorageAdapter* adapter = nullptr;
    auto backend = MakeObjectStorageBackend(adapter);
    ASSERT_TRUE(backend->Init());

    std::string value = "value";
    std::unordered_map<std::string, std::vector<Slice>> batch{
        {"key", {{value.data(), value.size()}}},
    };
    std::function<ErrorCode(const std::vector<std::string>&,
                            std::vector<StorageObjectMetadata>&)>
        complete_handler;

    auto result = backend->BatchOffload(batch, complete_handler);

    ASSERT_TRUE(result);
    EXPECT_EQ(*result, 1);
    EXPECT_EQ(adapter->objects.at("key"), value);
}

TEST_F(ObjectStorageAdapterTest, BatchOffloadRejectsTotalSizeOverflow) {
    FakeObjectStorageAdapter* adapter = nullptr;
    auto backend = MakeObjectStorageBackend(adapter);
    ASSERT_TRUE(backend->Init());

    char value = 'x';
    std::unordered_map<std::string, std::vector<Slice>> batch{
        {"overflow",
         {{&value, std::numeric_limits<size_t>::max()}, {&value, 1}}},
    };
    bool complete_handler_called = false;

    auto result =
        backend->BatchOffload(batch, [&](const std::vector<std::string>&,
                                         std::vector<StorageObjectMetadata>&) {
            complete_handler_called = true;
            return ErrorCode::OK;
        });

    ASSERT_TRUE(result);
    EXPECT_EQ(*result, 0);
    EXPECT_EQ(adapter->putv_calls, 0);
    EXPECT_FALSE(complete_handler_called);
}

TEST_F(ObjectStorageAdapterTest, BatchOffloadIgnoresEvictionHandler) {
    FakeObjectStorageAdapter* adapter = nullptr;
    auto backend = MakeObjectStorageBackend(adapter);
    ASSERT_TRUE(backend->Init());

    std::string value = "value";
    std::unordered_map<std::string, std::vector<Slice>> batch{
        {"key", {{value.data(), value.size()}}},
    };
    bool eviction_called = false;

    auto result = backend->BatchOffload(
        batch,
        [](const std::vector<std::string>&,
           std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; },
        [&](const std::vector<std::string>&) -> tl::expected<void, ErrorCode> {
            eviction_called = true;
            return {};
        });

    ASSERT_TRUE(result);
    EXPECT_EQ(*result, 1);
    EXPECT_FALSE(eviction_called);
}

TEST_F(ObjectStorageAdapterTest, BatchLoadReadsObjects) {
    FakeObjectStorageAdapter* adapter = nullptr;
    auto backend = MakeObjectStorageBackend(adapter);
    ASSERT_TRUE(backend->Init());
    adapter->objects["first"] = "hello";
    adapter->objects["second"] = "world";

    std::array<char, 5> first{};
    std::array<char, 5> second{};
    std::unordered_map<std::string, Slice> slices{
        {"first", {first.data(), first.size()}},
        {"second", {second.data(), second.size()}},
    };

    ASSERT_TRUE(backend->BatchLoad(slices));
    EXPECT_EQ(std::string(first.data(), first.size()), "hello");
    EXPECT_EQ(std::string(second.data(), second.size()), "world");
    EXPECT_EQ(adapter->get_calls, 2);
}

TEST_F(ObjectStorageAdapterTest, BatchLoadReturnsMissingKeyError) {
    FakeObjectStorageAdapter* adapter = nullptr;
    auto backend = MakeObjectStorageBackend(adapter);
    ASSERT_TRUE(backend->Init());

    std::array<char, 4> buffer{};
    std::unordered_map<std::string, Slice> slices{
        {"missing", {buffer.data(), buffer.size()}},
    };

    auto result = backend->BatchLoad(slices);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::FILE_NOT_FOUND);
}

TEST_F(ObjectStorageAdapterTest, IsExistUsesObjectStorageAdapter) {
    FakeObjectStorageAdapter* adapter = nullptr;
    auto backend = MakeObjectStorageBackend(adapter);
    ASSERT_TRUE(backend->Init());

    auto before = backend->IsExist("key");
    ASSERT_TRUE(before);
    EXPECT_FALSE(*before);

    adapter->objects["key"] = "value";
    auto after = backend->IsExist("key");
    ASSERT_TRUE(after);
    EXPECT_TRUE(*after);
}

TEST_F(ObjectStorageAdapterTest, ScanMetaBatchesKeysAndSizes) {
    FakeObjectStorageAdapter* adapter = nullptr;
    FileStorageConfig file_config;
    file_config.scanmeta_iterator_keys_limit = 2;
    auto backend = MakeObjectStorageBackend(adapter, file_config);
    ASSERT_TRUE(backend->Init());
    adapter->objects = {{"a", "1"}, {"bb", "22"}, {"ccc", "333"}};

    std::vector<size_t> batch_sizes;
    std::map<std::string, StorageObjectMetadata> metadata_by_key;
    auto result =
        backend->ScanMeta([&](const std::vector<std::string>& keys,
                              std::vector<StorageObjectMetadata>& metadata) {
            batch_sizes.push_back(keys.size());
            for (size_t i = 0; i < keys.size(); ++i) {
                metadata_by_key.emplace(keys[i], metadata[i]);
            }
            return ErrorCode::OK;
        });

    ASSERT_TRUE(result);
    EXPECT_EQ(batch_sizes, (std::vector<size_t>{2, 1}));
    ASSERT_EQ(metadata_by_key.size(), 3);
    EXPECT_EQ(metadata_by_key.at("a").key_size, 1);
    EXPECT_EQ(metadata_by_key.at("a").data_size, 1);
    EXPECT_EQ(metadata_by_key.at("bb").key_size, 2);
    EXPECT_EQ(metadata_by_key.at("bb").data_size, 2);
    EXPECT_EQ(metadata_by_key.at("ccc").key_size, 3);
    EXPECT_EQ(metadata_by_key.at("ccc").data_size, 3);
}

TEST_F(ObjectStorageAdapterTest, FileSystemModeKeepsDfsOnlyContract) {
    FileStorageConfig file_config;
    DistributedStorageConfig distributed_config;
    distributed_config.fsdir = root_dir_.string();
    auto owned_adapter = std::make_unique<FakeFileSystemAdapter>();
    DistributedStorageBackend backend(file_config, distributed_config,
                                      std::move(owned_adapter));

    EXPECT_EQ(backend.GetStorageMode(), DistributedStorageMode::kFileSystem);
    EXPECT_FALSE(backend.UsesObjectStorage());

    std::unordered_map<std::string, std::vector<Slice>> offload_batch;
    auto offload_result = backend.BatchOffload(
        offload_batch,
        [](const std::vector<std::string>&,
           std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; });
    ASSERT_FALSE(offload_result);
    EXPECT_EQ(offload_result.error(), ErrorCode::NOT_SUPPORTED);

    std::unordered_map<std::string, Slice> load_batch;
    auto load_result = backend.BatchLoad(load_batch);
    ASSERT_FALSE(load_result);
    EXPECT_EQ(load_result.error(), ErrorCode::NOT_SUPPORTED);

    auto exists_result = backend.IsExist("key");
    ASSERT_FALSE(exists_result);
    EXPECT_EQ(exists_result.error(), ErrorCode::NOT_SUPPORTED);

    auto scan_result = backend.ScanMeta(
        [](const std::vector<std::string>&,
           std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; });
    ASSERT_FALSE(scan_result);
    EXPECT_EQ(scan_result.error(), ErrorCode::NOT_SUPPORTED);
}

TEST_F(ObjectStorageAdapterTest, RejectsAmbiguousAdapterSelection) {
    FileStorageConfig file_config;
    DistributedStorageConfig distributed_config;
    distributed_config.fsdir = root_dir_.string();

    EXPECT_DEATH(
        {
            DistributedStorageBackend backend(
                file_config, distributed_config,
                std::make_unique<FakeFileSystemAdapter>(),
                std::make_unique<FakeObjectStorageAdapter>());
        },
        "exactly one I/O adapter is required");
}

}  // namespace

TEST_F(ObjectStorageAdapterTest, ObjectStorageConfigRequiresItsOwnFields) {
    DistributedStorageConfig config;
    config.fs_adapter_type = "object_storage";
    // An object namespace needs an identity and a transport. Without either there
    // is nothing to address and no way to reach it -- and a configuration that
    // validated here would fail much later, inside the adapter.
    EXPECT_FALSE(config.Validate());

    config.object_epoch = 7;
    config.kvclient_sdk_config_path = "/tmp/client.yaml";
    EXPECT_FALSE(config.Validate());

    config.object_cache_instance = "instance-a";
    EXPECT_TRUE(config.Validate());

    // The layout version is a contract, not a hint: this build only speaks 1.
    config.object_layout_version = 2;
    EXPECT_FALSE(config.Validate());
    config.object_layout_version = 1;
    EXPECT_TRUE(config.Validate());
}

TEST_F(ObjectStorageAdapterTest, TheAllocatorPathRefusesObjectStorageOutright) {
    DistributedStorageConfig config;
    config.fs_adapter_type = "object_storage";
    config.object_cache_instance = "instance-a";
    config.object_epoch = 7;
    config.kvclient_sdk_config_path = "/tmp/client.yaml";
    ASSERT_TRUE(config.Validate());

    // Valid as a *client* configuration, and still refused by the allocator path:
    // the Master's allocator is a filesystem allocator, and accepting this there
    // is what would hand `DfsGlobalAllocator::Init` a config it cannot build an
    // adapter for.
    EXPECT_FALSE(config.ValidateForAllocator());

    // The deployment shape the refusal requires: Master on posix, client on
    // object_storage. The posix side still validates for the allocator.
    DistributedStorageConfig master;
    master.fs_adapter_type = "posix";
    master.fsdir = std::filesystem::temp_directory_path().string();
    EXPECT_TRUE(master.Validate());
    EXPECT_TRUE(master.ValidateForAllocator());
}

namespace {
// The DFS object branches of BatchWrite/BatchRead take request-scoped
// descriptors, so these build them directly rather than going through
// BatchOffload/BatchLoad.
DfsWriteRequest WriteRequest(const std::string& key, uint64_t object_size,
                             const std::vector<Slice>& slices) {
    DfsWriteRequest request;
    request.key = key;
    request.descriptor.object_size = object_size;
    request.slices = slices;
    return request;
}

DfsReadRequest ReadRequest(const std::string& key, uint64_t object_size,
                           const std::vector<Slice>& slices) {
    DfsReadRequest request;
    request.key = key;
    request.descriptor.object_size = object_size;
    request.slices = slices;
    return request;
}
}  // namespace

TEST_F(ObjectStorageAdapterTest, ObjectBatchWriteRejectsEveryPositionOfADuplicateKey) {
    FakeObjectStorageAdapter* adapter = nullptr;
    auto backend = MakeObjectStorageBackend(adapter);
    ASSERT_TRUE(backend->Init());

    std::string dup_a = "aaaa";
    std::string unique = "cc";
    std::string dup_b = "bbbb";
    std::vector<DfsWriteRequest> requests = {
        WriteRequest("dup", 4, {{dup_a.data(), dup_a.size()}}),
        WriteRequest("unique", 2, {{unique.data(), unique.size()}}),
        WriteRequest("dup", 4, {{dup_b.data(), dup_b.size()}}),
    };
    auto results = backend->BatchWrite(requests);
    ASSERT_EQ(results.size(), 3u);
    // Every position naming the duplicated key is refused, not just the second.
    EXPECT_FALSE(results[0].has_value());
    EXPECT_TRUE(results[1].has_value());
    EXPECT_FALSE(results[2].has_value());
    // And neither duplicate reached the adapter: submitting both would let it
    // keep whichever it wrote last and report success twice for one object.
    EXPECT_EQ(adapter->objects.count("dup"), 0u);
    EXPECT_EQ(adapter->objects.at("unique"), "cc");
}

TEST_F(ObjectStorageAdapterTest, ObjectBatchWriteRejectsALengthMismatchPerItem) {
    FakeObjectStorageAdapter* adapter = nullptr;
    auto backend = MakeObjectStorageBackend(adapter);
    ASSERT_TRUE(backend->Init());

    std::string good = "aaaa";
    std::string short_by_one = "cc";
    std::vector<DfsWriteRequest> requests = {
        WriteRequest("good", 4, {{good.data(), good.size()}}),
        WriteRequest("bad", 3, {{short_by_one.data(), short_by_one.size()}}),
    };
    auto results = backend->BatchWrite(requests);
    ASSERT_EQ(results.size(), 2u);
    EXPECT_TRUE(results[0].has_value());
    EXPECT_FALSE(results[1].has_value());
    EXPECT_EQ(adapter->objects.at("good"), "aaaa");
    EXPECT_EQ(adapter->objects.count("bad"), 0u);
}

TEST_F(ObjectStorageAdapterTest, ObjectBatchReadClipsTheCallersTailGuard) {
    FakeObjectStorageAdapter* adapter = nullptr;
    auto backend = MakeObjectStorageBackend(adapter);
    ASSERT_TRUE(backend->Init());
    adapter->objects["k"] = "abcd";

    // The caller offers more capacity than the object needs. That is legal and
    // must not become a rejection; only the object's bytes may be written.
    std::vector<char> buffer(8, '\x7f');
    auto results = backend->BatchRead(
        {ReadRequest("k", 4, {{buffer.data(), buffer.size()}})});
    ASSERT_EQ(results.size(), 1u);
    EXPECT_TRUE(results[0].has_value());
    EXPECT_EQ(std::string(buffer.begin(), buffer.begin() + 4), "abcd");
    for (size_t i = 4; i < buffer.size(); ++i) {
        EXPECT_EQ(buffer[i], '\x7f') << "tail guard written at " << i;
    }
}

TEST_F(ObjectStorageAdapterTest, ObjectBatchReadRefusesInsufficientCapacity) {
    FakeObjectStorageAdapter* adapter = nullptr;
    auto backend = MakeObjectStorageBackend(adapter);
    ASSERT_TRUE(backend->Init());
    adapter->objects["k"] = "abcd";

    std::vector<char> too_small(3, '\0');
    auto results = backend->BatchRead(
        {ReadRequest("k", 4, {{too_small.data(), too_small.size()}})});
    ASSERT_EQ(results.size(), 1u);
    // The object does not fit. Refusing is the point: a short read would look
    // like success while delivering part of an object.
    EXPECT_FALSE(results[0].has_value());
}

TEST_F(ObjectStorageAdapterTest, ObjectBatchReadKeepsOneKeyInTwoDestinations) {
    FakeObjectStorageAdapter* adapter = nullptr;
    auto backend = MakeObjectStorageBackend(adapter);
    ASSERT_TRUE(backend->Init());
    adapter->objects["k"] = "abcd";

    // The same key twice into different destinations: both requests are real and
    // must not be collapsed into one.
    std::vector<char> first(4, '\0');
    std::vector<char> second(4, '\0');
    auto results = backend->BatchRead({
        ReadRequest("k", 4, {{first.data(), first.size()}}),
        ReadRequest("k", 4, {{second.data(), second.size()}}),
    });
    ASSERT_EQ(results.size(), 2u);
    EXPECT_TRUE(results[0].has_value());
    EXPECT_TRUE(results[1].has_value());
    EXPECT_EQ(std::string(first.begin(), first.end()), "abcd");
    EXPECT_EQ(std::string(second.begin(), second.end()), "abcd");
}

TEST_F(ObjectStorageAdapterTest, ObjectBatchReadZeroLengthRegionsAreSkipped) {
    FakeObjectStorageAdapter* adapter = nullptr;
    auto backend = MakeObjectStorageBackend(adapter);
    ASSERT_TRUE(backend->Init());
    adapter->objects["k"] = "abcd";

    std::vector<char> whole(4, '\0');
    auto results = backend->BatchRead({ReadRequest(
        "k", 4, {{nullptr, 0}, {whole.data(), whole.size()}, {nullptr, 0}})});
    ASSERT_EQ(results.size(), 1u);
    EXPECT_TRUE(results[0].has_value());
    EXPECT_EQ(std::string(whole.begin(), whole.end()), "abcd");
}

namespace {

// The batch contract, exercised through the default (serial) bodies. What is
// asserted here is the *contract*, not the default's concurrency: the default is
// documented as serial, and an adapter that can submit a whole batch must
// override it. Results must be per item, in order, and one bad item must not
// suppress its neighbours.
class BatchContractTest : public ::testing::Test {
   protected:
    FakeObjectStorageAdapter adapter_;
};

TEST_F(BatchContractTest, EmptyBatchYieldsEmptyResults) {
    EXPECT_EQ(adapter_.PutVBatch({}).size(), 0u);
    EXPECT_EQ(adapter_.GetBatch({}).size(), 0u);
}

TEST_F(BatchContractTest, ResultsMatchRequestsInLengthAndOrder) {
    std::string a("aaaa"), b("bb"), c("cccccc");
    std::vector<ObjectPutRequest> puts = {
        {"k1", {{a.data(), a.size()}}},
        {"k2", {{b.data(), 1}, {b.data() + 1, 1}}},
        {"k3", {{c.data(), c.size()}}},
    };
    auto put_results = adapter_.PutVBatch(puts);
    EXPECT_EQ(put_results.size(), puts.size());
    for (const auto& r : put_results) EXPECT_TRUE(r.has_value());
    EXPECT_EQ(adapter_.objects["k1"], "aaaa");
    EXPECT_EQ(adapter_.objects["k2"], "bb");
    EXPECT_EQ(adapter_.objects["k3"], "cccccc");
}

TEST_F(BatchContractTest, OneFailedItemDoesNotSuppressTheOthers) {
    std::string a("aaaa"), b("bb"), c("cccccc");
    adapter_.fail_keys.insert("k2");
    std::vector<ObjectPutRequest> puts = {
        {"k1", {{a.data(), a.size()}}},
        {"k2", {{b.data(), b.size()}}},
        {"k3", {{c.data(), c.size()}}},
    };
    auto results = adapter_.PutVBatch(puts);
    ASSERT_EQ(results.size(), 3u);
    EXPECT_TRUE(results[0].has_value());
    EXPECT_FALSE(results[1].has_value());
    EXPECT_TRUE(results[2].has_value());
}

TEST_F(BatchContractTest, ReadsTheWholeObjectAcrossSeveralRegions) {
    adapter_.objects["k"] = "abcdef";
    std::vector<char> head(3), tail(3);
    std::vector<ObjectGetRequest> gets = {
        {"k", {{head.data(), head.size()}, {tail.data(), tail.size()}}, 6}};
    auto results = adapter_.GetBatch(gets);
    ASSERT_EQ(results.size(), 1u);
    ASSERT_TRUE(results[0].has_value());
    EXPECT_EQ(*results[0], 6u);
    EXPECT_EQ(std::string(head.begin(), head.end()), "abc");
    EXPECT_EQ(std::string(tail.begin(), tail.end()), "def");
}

TEST_F(BatchContractTest, ADestinationThatCannotHoldTheObjectIsRefused) {
    // The object is four bytes; the caller offers three. Refusing is the whole
    // point: a truncated read reports a length that looks like success.
    adapter_.objects["k"] = "abcd";
    std::vector<char> small(3);
    std::vector<ObjectGetRequest> gets = {{"k", {{small.data(), small.size()}}, 3}};
    auto results = adapter_.GetBatch(gets);
    ASSERT_EQ(results.size(), 1u);
    EXPECT_FALSE(results[0].has_value());
}

TEST_F(BatchContractTest, ExpectedLengthMismatchIsRefusedPerItem) {
    adapter_.objects["k1"] = "abcd";
    adapter_.objects["k2"] = "abcdef";
    std::vector<char> g1(4), b2(3), g3(2);
    std::vector<ObjectGetRequest> gets = {
        {"k1", {{g1.data(), g1.size()}}, 4},
        {"k2", {{b2.data(), b2.size()}}, 3},   // regions do not cover 3 bytes of a 6-byte object correctly
        {"k2", {{g3.data(), g3.size()}}, 2},   // and a second, independent item
    };
    auto results = adapter_.GetBatch(gets);
    ASSERT_EQ(results.size(), 3u);
    EXPECT_TRUE(results[0].has_value());
    EXPECT_FALSE(results[1].has_value());
    EXPECT_FALSE(results[2].has_value());
}

TEST_F(BatchContractTest, IovLengthSumOverflowIsRefusedNotWrapped) {
    // SIZE_MAX + 2 wraps to 1. Without the check, an expected_length of 1 would
    // accept it and the copy would run off the end.
    std::vector<char> byte(1);
    std::vector<iovec> iovs = {
        {byte.data(), std::numeric_limits<size_t>::max()},
        {byte.data(), 2},
    };
    std::vector<ObjectPutRequest> puts = {{"k", iovs}};
    auto results = adapter_.PutVBatch(puts);
    ASSERT_EQ(results.size(), 1u);
    EXPECT_FALSE(results[0].has_value());

    std::vector<ObjectGetRequest> gets = {{"k", iovs, 1}};
    auto get_results = adapter_.GetBatch(gets);
    ASSERT_EQ(get_results.size(), 1u);
    EXPECT_FALSE(get_results[0].has_value());
}

TEST_F(BatchContractTest, NonZeroLengthWithNullBaseIsRefused) {
    std::vector<char> byte(1);
    std::vector<iovec> iovs = {
        {nullptr, 8},
        {byte.data(), 1},
    };
    auto results = adapter_.PutVBatch({{"k", iovs}});
    ASSERT_EQ(results.size(), 1u);
    EXPECT_FALSE(results[0].has_value());
}

TEST_F(BatchContractTest, ZeroLengthRegionsAreSkippedSafely) {
    std::string a("aaaa"), b("bb");
    std::vector<iovec> iovs = {
        {a.data(), a.size()},
        {nullptr, 0},  // a zero-length region is legal even with a null base
        {b.data(), b.size()},
    };
    auto results = adapter_.PutVBatch({{"k", iovs}});
    ASSERT_EQ(results.size(), 1u);
    EXPECT_TRUE(results[0].has_value());
    EXPECT_EQ(adapter_.objects["k"], "aaaabb");
}

TEST_F(BatchContractTest, AnEmptyObjectIsRefusedRatherThanTreatedAsSuccess) {
    std::vector<iovec> none;
    auto put_results = adapter_.PutVBatch({{"k", none}});
    ASSERT_EQ(put_results.size(), 1u);
    EXPECT_FALSE(put_results[0].has_value());

    auto get_results = adapter_.GetBatch({{"k", none, 0}});
    ASSERT_EQ(get_results.size(), 1u);
    EXPECT_FALSE(get_results[0].has_value());
}

TEST_F(BatchContractTest, ASingleAllocationFailureDoesNotEndTheBatch) {
    // Each item is staged separately, so an over-budget item fails on its own
    // rather than by terminating the call.
    std::vector<char> byte(1);
    std::vector<ObjectGetRequest> gets = {
        {"k", {{byte.data(), byte.size()}}, byte.size()},
        {"k", {{byte.data(), ObjectStorageAdapter::kDefaultBatchScratchBudgetBytes + 1}},
              ObjectStorageAdapter::kDefaultBatchScratchBudgetBytes + 1},
        {"k", {{byte.data(), byte.size()}}, byte.size()},
    };
    adapter_.objects["k"] = "x";
    auto results = adapter_.GetBatch(gets);
    ASSERT_EQ(results.size(), 3u);
    EXPECT_TRUE(results[0].has_value());
    EXPECT_FALSE(results[1].has_value());
    EXPECT_TRUE(results[2].has_value());
}

}  // namespace

}  // namespace mooncake
