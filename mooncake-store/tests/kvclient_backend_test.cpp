// Copyright (c) 2026 Hygon Information Technology Co., Ltd.
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <spawn.h>
#include <stdexcept>
#include <string>
#include <sys/wait.h>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <unistd.h>

#include "kvclient_backend.h"
#include "storage_backend.h"

extern char** environ;

namespace mooncake::test {
namespace fs = std::filesystem;
namespace {

constexpr const char* kConfigPathEnv =
    "MOONCAKE_OFFLOAD_KVCLIENT_CONFIG_PATH";
constexpr const char* kIndexPathEnv = "MOONCAKE_OFFLOAD_KVCLIENT_INDEX_PATH";
constexpr const char* kDevicePayloadsEnv =
    "MOONCAKE_OFFLOAD_KVCLIENT_DEVICE_PAYLOADS";
constexpr size_t kRecoveryObjects = 300;

std::optional<std::string> ArrayConfigPath() {
    const char* value = std::getenv(kConfigPathEnv);
    if (value == nullptr || value[0] == '\0') return std::nullopt;
    return std::string(value);
}

bool RequireArray() {
    const char* value = std::getenv("MOONCAKE_KVCLIENT_TEST_REQUIRE_ARRAY");
    return value != nullptr && std::string(value) == "1";
}

class ScopedEnvVar {
   public:
    explicit ScopedEnvVar(const char* name) : name_(name) {
        const char* value = std::getenv(name);
        if (value != nullptr) original_ = value;
        unsetenv(name);
    }
    ~ScopedEnvVar() {
        if (original_.has_value()) {
            setenv(name_.c_str(), original_->c_str(), 1);
        } else {
            unsetenv(name_.c_str());
        }
    }
    ScopedEnvVar(const ScopedEnvVar&) = delete;
    ScopedEnvVar& operator=(const ScopedEnvVar&) = delete;
    void Set(const std::string& value) { setenv(name_.c_str(), value.c_str(), 1); }

   private:
    std::string name_;
    std::optional<std::string> original_;
};

fs::path NewIndexDirectory() {
    std::string pattern =
        (fs::temp_directory_path() / "mooncake-kvclient-test-XXXXXX").string();
    std::vector<char> buffer(pattern.begin(), pattern.end());
    buffer.push_back('\0');
    const char* result = ::mkdtemp(buffer.data());
    if (result == nullptr) throw std::runtime_error("mkdtemp failed");
    return fs::path(result);
}

std::vector<char> Payload(size_t object, size_t size = 4096) {
    std::vector<char> result(size);
    for (size_t i = 0; i < size; ++i) {
        result[i] = static_cast<char>((object * 17 + i * 31 + 7) & 0xff);
    }
    return result;
}

std::string RecoveryKey(size_t object) {
    return "restart-object-" + std::to_string(object);
}

ErrorCode AcceptOffload(const std::vector<std::string>&,
                        std::vector<StorageObjectMetadata>&) {
    return ErrorCode::OK;
}

// 子进程重新 exec，不继承已启动的 SDK 线程、RDMA 会话或索引锁。
int RunRestartWorker(const std::string& phase) {
    ScopedEnvVar worker_phase{"MOONCAKE_KVCLIENT_TEST_PHASE"};
    // 父测试的分片/报告配置不能让工作进程漏跑或覆盖父进程的 XML 报告。
    ScopedEnvVar shard_count{"GTEST_TOTAL_SHARDS"};
    ScopedEnvVar shard_index{"GTEST_SHARD_INDEX"};
    ScopedEnvVar shard_status{"GTEST_SHARD_STATUS_FILE"};
    ScopedEnvVar output{"GTEST_OUTPUT"};
    worker_phase.Set(phase);
    std::string executable = "/proc/self/exe";
    std::string filter =
        "--gtest_filter=KvclientBackendProcessTest.DISABLED_RestartWorker";
    std::string disabled = "--gtest_also_run_disabled_tests";
    std::string repeat = "--gtest_repeat=1";
    char* arguments[] = {executable.data(), filter.data(), disabled.data(),
                         repeat.data(), nullptr};
    pid_t child = -1;
    const int spawned =
        ::posix_spawn(&child, executable.c_str(), nullptr, nullptr, arguments,
                      environ);
    if (spawned != 0) return -spawned;
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(180);
    while (std::chrono::steady_clock::now() < deadline) {
        int status = 0;
        const pid_t waited = ::waitpid(child, &status, WNOHANG);
        if (waited == child) {
            return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
        }
        if (waited < 0 && errno != EINTR) return -1;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ::kill(child, SIGKILL);
    int status = 0;
    while (::waitpid(child, &status, 0) < 0 && errno == EINTR) {
    }
    return -ETIMEDOUT;
}

}  // namespace

// 配置失败必须在没有阵列的开发环境中也得到覆盖。
TEST(KvclientBackendConfigTest, FactoryRejectsMissingSdkConfigPath) {
    ScopedEnvVar config_path{kConfigPathEnv};
    FileStorageConfig config;
    config.storage_backend_type = StorageBackendType::kKvclient;
    EXPECT_THROW(CreateStorageBackend(config), std::invalid_argument);
}

TEST(KvclientBackendConfigTest, FactoryRejectsUnregisteredDevicePayloads) {
    ScopedEnvVar config_path{kConfigPathEnv};
    ScopedEnvVar device{kDevicePayloadsEnv};
    config_path.Set("/unused/client.yaml");
    device.Set("true");
    FileStorageConfig config;
    config.storage_backend_type = StorageBackendType::kKvclient;
    EXPECT_THROW(CreateStorageBackend(config), std::invalid_argument);
}

TEST(KvclientBackendConfigTest, ExplicitIndexPathMustBeAbsolute) {
    KvclientBackendConfig config;
    config.sdk_config_path = "/unused/client.yaml";
    config.index_path = "relative/client.index";
    EXPECT_FALSE(config.Validate());
    config.index_path = "/persistent/store-a/client.index";
    EXPECT_TRUE(config.Validate());
}

TEST(KvclientBackendConfigTest, EnvironmentSelectsBackendAndStableIndex) {
    ScopedEnvVar descriptor{"MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR"};
    ScopedEnvVar index_path{kIndexPathEnv};
    descriptor.Set("kvclient_storage_backend");
    index_path.Set("/persistent/store-a/client.index");
    EXPECT_EQ(FileStorageConfig::FromEnvironment().storage_backend_type,
              StorageBackendType::kKvclient);
    EXPECT_EQ(KvclientBackendConfig::FromEnvironment().index_path,
              "/persistent/store-a/client.index");
}

class KvclientBackendTest : public ::testing::Test {
   protected:
    void SetUp() override {
        const auto config_path = ArrayConfigPath();
        if (!config_path.has_value()) {
            ASSERT_FALSE(RequireArray()) << kConfigPathEnv << " is required";
            GTEST_SKIP() << "set " << kConfigPathEnv
                         << " to run against a real array";
        }
        index_directory_ = NewIndexDirectory();
        config_.storage_backend_type = StorageBackendType::kKvclient;
        config_.storage_filepath = index_directory_.string();
        config_.enable_disk_watermark_eviction = false;
        kvclient_config_.sdk_config_path = *config_path;
        kvclient_config_.prefix = index_directory_.filename().string();
        kvclient_config_.layout = "v1";
        backend_ = std::make_shared<KvclientStorageBackend>(config_,
                                                          kvclient_config_);
        const auto init = backend_->Init();
        ASSERT_TRUE(init.has_value()) << "Init failed: " << init.error();
    }

    void TearDown() override {
        if (backend_ != nullptr) {
            backend_->RemoveAll();
            backend_.reset();
        }
        if (!index_directory_.empty()) {
            std::error_code ignored;
            fs::remove_all(index_directory_, ignored);
        }
    }

    void Put(const std::string& key, std::vector<char> payload) {
        std::unordered_map<std::string, std::vector<Slice>> batch;
        batch.emplace(key, std::vector<Slice>{Slice{payload.data(), payload.size()}});
        const auto result = backend_->BatchOffload(batch, AcceptOffload);
        ASSERT_TRUE(result.has_value()) << result.error();
        EXPECT_EQ(result.value(), 1);
    }

    std::shared_ptr<StorageBackendInterface> backend_;
    FileStorageConfig config_;
    KvclientBackendConfig kvclient_config_;
    fs::path index_directory_;
};

TEST_F(KvclientBackendTest, ReportsOffloadingEnabled) {
    const auto enabled = backend_->IsEnableOffloading();
    ASSERT_TRUE(enabled.has_value()) << enabled.error();
    EXPECT_TRUE(enabled.value());
}

// 单值上限是 max_object_bytes（默认且最大 4 MiB），所以合法边界是"刚好在
// 上限之内"。多切片来源仍然允许：它们被拼成一个 Value，只要总长不超上限。
TEST_F(KvclientBackendTest, OffloadThenLoadRoundTripsMultipleSlices) {
    auto payload = Payload(3, 4 * 1024 * 1024 - 257);
    std::unordered_map<std::string, std::vector<Slice>> batch;
    batch.emplace("round-trip", std::vector<Slice>{
                                    Slice{payload.data(), 173},
                                    Slice{payload.data() + 173,
                                          payload.size() - 173}});
    std::vector<std::string> completed;
    const auto offload = backend_->BatchOffload(
        batch, [&completed, &payload](const std::vector<std::string>& keys,
                                      std::vector<StorageObjectMetadata>& meta) {
            completed = keys;
            EXPECT_EQ(meta.size(), keys.size());
            if (!meta.empty()) {
                EXPECT_EQ(meta[0].data_size, payload.size());
            }
            return ErrorCode::OK;
        });
    ASSERT_TRUE(offload.has_value()) << offload.error();
    EXPECT_EQ(offload.value(), 1);
    EXPECT_EQ(completed, std::vector<std::string>{"round-trip"});
    const auto exists = backend_->IsExist("round-trip");
    ASSERT_TRUE(exists.has_value());
    EXPECT_TRUE(*exists);
    std::vector<char> restored(payload.size(), 0);
    std::unordered_map<std::string, Slice> slices;
    slices.emplace("round-trip", Slice{restored.data(), restored.size()});
    const auto load = backend_->BatchLoad(slices);
    ASSERT_TRUE(load.has_value()) << load.error();
    EXPECT_EQ(restored, payload);
}

// 超过单值上限的对象必须在**提交前**被拒绝：不能写远端、不能进索引、不能回调。
// 旧契约里 4 MiB+257 是被接受的（当时按分片切），新单值格式下它超限，
// 因此这里同时锁住"拒绝"和"拒绝得干净"两件事。
TEST_F(KvclientBackendTest, OverLimitValueIsRejectedBeforeAnyWrite) {
    const auto limit = 4 * 1024 * 1024;
    auto payload = Payload(3, limit + 257);
    std::unordered_map<std::string, std::vector<Slice>> batch;
    batch.emplace("over-limit", std::vector<Slice>{
                                    Slice{payload.data(), 173},
                                    Slice{payload.data() + 173,
                                          payload.size() - 173}});
    bool callback_ran = false;
    const auto offload = backend_->BatchOffload(
        batch, [&callback_ran](const std::vector<std::string>&,
                               std::vector<StorageObjectMetadata>&) {
            callback_ran = true;
            return ErrorCode::OK;
        });
    ASSERT_FALSE(offload.has_value());
    EXPECT_EQ(offload.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_FALSE(callback_ran) << "超限对象不得触发完成回调";

    // 远端没有对象，索引里也没有条目：扫描不到，读也读不到。
    const auto exists = backend_->IsExist("over-limit");
    ASSERT_TRUE(exists.has_value()) << exists.error();
    EXPECT_FALSE(exists.value());
    std::vector<std::string> scanned;
    const auto scan = backend_->ScanMeta(
        [&scanned](const std::vector<std::string>& keys,
                   std::vector<StorageObjectMetadata>&) {
            scanned.insert(scanned.end(), keys.begin(), keys.end());
            return ErrorCode::OK;
        });
    ASSERT_TRUE(scan.has_value()) << scan.error();
    EXPECT_EQ(std::find(scanned.begin(), scanned.end(), "over-limit"),
              scanned.end());

    // 同长度上限内的对象仍然可以正常往返，说明拒绝的是长度而不是这一批。
    auto ok_payload = Payload(3, limit - 257);
    std::unordered_map<std::string, std::vector<Slice>> ok_batch;
    ok_batch.emplace("at-limit", std::vector<Slice>{
                                     Slice{ok_payload.data(), ok_payload.size()}});
    const auto accepted = backend_->BatchOffload(ok_batch, AcceptOffload);
    ASSERT_TRUE(accepted.has_value()) << accepted.error();
    EXPECT_EQ(accepted.value(), 1);
}

TEST_F(KvclientBackendTest, UnknownKeyIsAbsentAndCannotBeLoaded) {
    const auto exists = backend_->IsExist("absent");
    ASSERT_TRUE(exists.has_value());
    EXPECT_FALSE(*exists);
    std::vector<char> buffer(64, 0);
    std::unordered_map<std::string, Slice> slices;
    slices.emplace("absent", Slice{buffer.data(), buffer.size()});
    EXPECT_FALSE(backend_->BatchLoad(slices).has_value());
}

TEST_F(KvclientBackendTest, ScanFailureCanRetryWithoutLosingObjects) {
    ASSERT_NO_FATAL_FAILURE(Put("scan-a", Payload(0)));
    ASSERT_NO_FATAL_FAILURE(Put("scan-b", Payload(1)));
    backend_->ResetScanIterator();
    size_t failed_calls = 0;
    const auto failure = backend_->ScanMeta(
        [&failed_calls](const std::vector<std::string>&,
                        std::vector<StorageObjectMetadata>&) {
            ++failed_calls;
            return ErrorCode::INTERNAL_ERROR;
        });
    ASSERT_FALSE(failure.has_value());
    EXPECT_EQ(failure.error(), ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(failed_calls, 1U);
    std::unordered_set<std::string> scanned;
    const auto retry = backend_->ScanMeta(
        [&scanned](const std::vector<std::string>& keys,
                   std::vector<StorageObjectMetadata>&) {
            scanned.insert(keys.begin(), keys.end());
            return ErrorCode::OK;
        });
    ASSERT_TRUE(retry.has_value()) << retry.error();
    EXPECT_EQ(scanned, (std::unordered_set<std::string>{"scan-a", "scan-b"}));
}

TEST_F(KvclientBackendTest, RejectsSimultaneousOwnersOfOneIndex) {
    auto duplicate =
        std::make_shared<KvclientStorageBackend>(config_, kvclient_config_);
    const auto init = duplicate->Init();
    EXPECT_FALSE(init.has_value());
    duplicate.reset();
    ASSERT_NO_FATAL_FAILURE(Put("original-owner-still-works", Payload(4)));
}

TEST_F(KvclientBackendTest, FactoryReopensTheDefaultStableIndex) {
    ASSERT_NO_FATAL_FAILURE(Put("factory-recovery", Payload(5)));
    const std::string sdk_path = kvclient_config_.sdk_config_path;
    backend_.reset();
    ScopedEnvVar config_path{kConfigPathEnv};
    ScopedEnvVar index_path{kIndexPathEnv};
    ScopedEnvVar index_dir{"MOONCAKE_OFFLOAD_KVCLIENT_INDEX_DIR"};
    ScopedEnvVar prefix{"MOONCAKE_OFFLOAD_KVCLIENT_PREFIX"};
    ScopedEnvVar layout{"MOONCAKE_OFFLOAD_KVCLIENT_LAYOUT"};
    ScopedEnvVar device{kDevicePayloadsEnv};
    config_path.Set(sdk_path);
    prefix.Set(kvclient_config_.prefix);
    layout.Set(kvclient_config_.layout);
    const auto reopened = CreateStorageBackend(config_);
    ASSERT_TRUE(reopened.has_value()) << reopened.error();
    backend_ = *reopened;
    const auto init = backend_->Init();
    ASSERT_TRUE(init.has_value()) << init.error();
    std::vector<std::string> scanned;
    const auto scan = backend_->ScanMeta(
        [&scanned](const std::vector<std::string>& keys,
                   std::vector<StorageObjectMetadata>&) {
            scanned.insert(scanned.end(), keys.begin(), keys.end());
            return ErrorCode::OK;
        });
    ASSERT_TRUE(scan.has_value()) << scan.error();
    EXPECT_EQ(scanned, std::vector<std::string>{"factory-recovery"});
}

// 默认不执行；只在显式选择此项时产生三批隔离的读写负载。
TEST_F(KvclientBackendTest, DISABLED_RealisticThroughput) {
    constexpr size_t kBatches = 3;
    constexpr size_t kObjectsPerBatch = 128;
    constexpr size_t kObjectBytes = 1280 * 1024;
    double put_seconds = 0.0;
    double get_seconds = 0.0;
    const auto report = [](const char* operation, const std::string& batch,
                           size_t objects, double seconds) {
        const double mib = static_cast<double>(objects) * kObjectBytes /
                           (1024.0 * 1024.0);
        std::printf(
            "KVCLIENT_BENCH op=%s batch=%s objects=%zu object_bytes=%zu "
            "seconds=%.6f objs_per_sec=%.3f mib_per_sec=%.3f\n",
            operation, batch.c_str(), objects, kObjectBytes, seconds,
            objects / seconds, mib / seconds);
        std::fflush(stdout);
    };

    std::printf("KVCLIENT_BENCH prefix=%s index_dir=%s batches=%zu "
                "objects_per_batch=%zu object_bytes=%zu\n",
                kvclient_config_.prefix.c_str(), index_directory_.c_str(),
                kBatches, kObjectsPerBatch, kObjectBytes);
    std::fflush(stdout);
    for (size_t batch = 0; batch < kBatches; ++batch) {
        std::vector<std::vector<char>> payloads;
        std::vector<std::vector<char>> restored;
        payloads.reserve(kObjectsPerBatch);
        restored.reserve(kObjectsPerBatch);
        std::unordered_map<std::string, std::vector<Slice>> writes;
        std::unordered_map<std::string, Slice> reads;
        for (size_t object = 0; object < kObjectsPerBatch; ++object) {
            const uint64_t object_id = batch * kObjectsPerBatch + object;
            payloads.push_back(Payload(object_id, kObjectBytes));
            // 写入完整 ID，避免测试数据按字节周期重复而掩盖串 Key。
            std::memcpy(payloads.back().data(), &object_id, sizeof(object_id));
            restored.emplace_back(kObjectBytes, 0);
            const std::string key = "throughput-" + std::to_string(object_id);
            writes.emplace(key, std::vector<Slice>{Slice{
                                    payloads.back().data(), kObjectBytes}});
            reads.emplace(key, Slice{restored.back().data(), kObjectBytes});
        }

        const auto put_start = std::chrono::steady_clock::now();
        const auto put = backend_->BatchOffload(writes, AcceptOffload);
        const double put_elapsed = std::chrono::duration<double>(
                                       std::chrono::steady_clock::now() - put_start)
                                       .count();
        ASSERT_TRUE(put.has_value()) << "batch=" << batch << ": " << put.error();
        ASSERT_EQ(put.value(), kObjectsPerBatch);
        ASSERT_GT(put_elapsed, 0.0);
        put_seconds += put_elapsed;
        report("PUT", std::to_string(batch + 1), kObjectsPerBatch, put_elapsed);

        const auto get_start = std::chrono::steady_clock::now();
        const auto get = backend_->BatchLoad(reads);
        const double get_elapsed = std::chrono::duration<double>(
                                       std::chrono::steady_clock::now() - get_start)
                                       .count();
        ASSERT_TRUE(get.has_value()) << "batch=" << batch << ": " << get.error();
        ASSERT_GT(get_elapsed, 0.0);
        for (size_t object = 0; object < kObjectsPerBatch; ++object) {
            ASSERT_EQ(restored[object], payloads[object])
                << "batch=" << batch << " object=" << object;
        }
        get_seconds += get_elapsed;
        report("GET", std::to_string(batch + 1), kObjectsPerBatch, get_elapsed);
    }
    report("PUT", "total", kBatches * kObjectsPerBatch, put_seconds);
    report("GET", "total", kBatches * kObjectsPerBatch, get_seconds);
    std::printf("KVCLIENT_BENCH content_verified=true objects=%zu\n",
                kBatches * kObjectsPerBatch);
    std::fflush(stdout);
}

TEST(KvclientBackendProcessTest, RecoversAllObjectsAcrossDifferentPids) {
    if (!ArrayConfigPath().has_value()) {
        ASSERT_FALSE(RequireArray()) << kConfigPathEnv << " is required";
        GTEST_SKIP() << "set " << kConfigPathEnv << " to run against a real array";
    }
    const fs::path directory = NewIndexDirectory();
    ScopedEnvVar worker_dir{"MOONCAKE_KVCLIENT_TEST_DIRECTORY"};
    worker_dir.Set(directory.string());
    ASSERT_EQ(RunRestartWorker("write"), 0)
        << "writer failed; index retained at " << directory;
    ASSERT_EQ(RunRestartWorker("read"), 0)
        << "reader failed; index retained at " << directory;
    std::error_code ignored;
    fs::remove_all(directory, ignored);
}

// 只由上面的父进程以指定环境执行，普通运行不重复执行该工作进程。
TEST(KvclientBackendProcessTest, DISABLED_RestartWorker) {
    const char* directory = std::getenv("MOONCAKE_KVCLIENT_TEST_DIRECTORY");
    const char* phase = std::getenv("MOONCAKE_KVCLIENT_TEST_PHASE");
    ASSERT_NE(directory, nullptr);
    ASSERT_NE(phase, nullptr);
    ASSERT_TRUE(ArrayConfigPath().has_value());
    FileStorageConfig config;
    config.storage_backend_type = StorageBackendType::kKvclient;
    config.storage_filepath = directory;
    config.enable_disk_watermark_eviction = false;
    KvclientBackendConfig adapter;
    adapter.sdk_config_path = *ArrayConfigPath();
    adapter.index_path = (fs::path(directory) / "stable-client.index").string();
    adapter.prefix = fs::path(directory).filename().string();
    auto backend = std::make_unique<KvclientStorageBackend>(config, adapter);
    const auto init = backend->Init();
    ASSERT_TRUE(init.has_value()) << init.error();

    if (std::string(phase) == "write") {
        std::vector<std::vector<char>> payloads;
        payloads.reserve(kRecoveryObjects);
        std::unordered_map<std::string, std::vector<Slice>> batch;
        for (size_t i = 0; i < kRecoveryObjects; ++i) {
            payloads.push_back(Payload(i, 64 + i % 17));
            auto& bytes = payloads.back();
            batch.emplace(RecoveryKey(i),
                          std::vector<Slice>{Slice{bytes.data(), bytes.size()}});
        }
        const auto write = backend->BatchOffload(batch, AcceptOffload);
        ASSERT_TRUE(write.has_value()) << write.error();
        EXPECT_EQ(write.value(), kRecoveryObjects);
        std::ofstream writer_pid(fs::path(directory) / "writer.pid");
        writer_pid << ::getpid();
        writer_pid.close();
        ASSERT_TRUE(writer_pid.good());
        return;
    }
    ASSERT_EQ(std::string(phase), "read");
    std::ifstream writer_pid(fs::path(directory) / "writer.pid");
    pid_t original_pid = -1;
    ASSERT_TRUE(static_cast<bool>(writer_pid >> original_pid));
    ASSERT_NE(original_pid, ::getpid());
    std::unordered_set<std::string> scanned;
    const auto scan = backend->ScanMeta(
        [&scanned](const std::vector<std::string>& keys,
                   std::vector<StorageObjectMetadata>& metadata) {
            EXPECT_EQ(keys.size(), metadata.size());
            for (size_t i = 0; i < keys.size(); ++i) {
                EXPECT_TRUE(scanned.insert(keys[i]).second) << keys[i];
                if (i < metadata.size()) {
                    EXPECT_EQ(metadata[i].key_size, keys[i].size());
                }
            }
            return ErrorCode::OK;
        });
    ASSERT_TRUE(scan.has_value()) << scan.error();
    ASSERT_EQ(scanned.size(), kRecoveryObjects);
    for (size_t i = 0; i < kRecoveryObjects; ++i) {
        ASSERT_EQ(scanned.count(RecoveryKey(i)), 1U);
        const auto expected = Payload(i, 64 + i % 17);
        std::vector<char> restored(expected.size(), 0);
        std::unordered_map<std::string, Slice> slices;
        slices.emplace(RecoveryKey(i), Slice{restored.data(), restored.size()});
        const auto load = backend->BatchLoad(slices);
        ASSERT_TRUE(load.has_value()) << RecoveryKey(i) << ": " << load.error();
        EXPECT_EQ(restored, expected) << RecoveryKey(i);
    }
    size_t repeated = 0;
    auto count = [&repeated](const std::vector<std::string>& keys,
                             std::vector<StorageObjectMetadata>&) {
        repeated += keys.size();
        return ErrorCode::OK;
    };
    ASSERT_TRUE(backend->ScanMeta(count).has_value());
    EXPECT_EQ(repeated, 0U);
    backend->ResetScanIterator();
    ASSERT_TRUE(backend->ScanMeta(count).has_value());
    EXPECT_EQ(repeated, kRecoveryObjects);
    backend->RemoveAll();
}

}  // namespace mooncake::test
