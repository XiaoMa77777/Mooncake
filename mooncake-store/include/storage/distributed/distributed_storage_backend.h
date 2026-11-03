#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "fs_adapter.h"
#include "replica.h"
#include "storage/distributed/object_storage_adapter.h"
#include "storage_backend.h"

namespace mooncake {

// Filesystem mode is the descriptor-based shard/offset DFS data path. Object
// storage remains a separate, logical-key-oriented I/O mode.
enum class DistributedStorageMode {
    kFileSystem,
    kObjectStorage,
};

struct DistributedStorageConfig {
    std::string fsdir = "/mnt/3fs/mooncake";
    std::string fs_adapter_type = "hf3fs";
    bool enable_health_check = false;
    int shard_count = 64;
    uint64_t shard_capacity = 4ULL * 1024 * 1024 * 1024;
    uint64_t alignment = 4096;
    bool single_tenant = true;
    bool eviction_enabled = true;
    double eviction_high_watermark = 0.9;
    double eviction_low_watermark = 0.7;
    std::chrono::seconds deferred_free_duration{30};
    std::chrono::seconds eviction_check_interval{5};

    // Object-storage mode (`fs_adapter_type == "object_storage"`). These are
    // *client* settings: the Master's allocator stays a filesystem allocator, so
    // a deployment runs the Master on hf3fs/posix and the client on
    // object_storage. `ValidateForAllocator` refuses this mode outright for that
    // reason -- see its comment.
    //
    // `object_cache_instance` and `object_epoch` are supplied by deployment
    // orchestration, not generated per process: every client of one instance must
    // agree on the epoch, and a Master restart requires stopping the old clients
    // and switching the whole instance to a new epoch before restarting.
    std::string object_cache_instance;
    uint64_t object_epoch = 0;
    uint32_t object_layout_version = 1;
    size_t object_duplicate_window_keys = 1u << 16;
    // "off" | "diagnostic" | "strict"; mapped to the core's enum by the factory.
    std::string object_duplicate_admission = "diagnostic";
    size_t object_scratch_budget_bytes = 64ULL * 1024ULL * 1024ULL;
    std::string kvclient_sdk_config_path;
    uint32_t kvclient_timeout_ms = 5000;

    bool Validate() const;
    bool ValidateForAllocator() const;
    static DistributedStorageConfig FromEnvironment();
    std::string FormatStr() const;
};

struct DfsWriteRequest {
    std::string key;
    DistributedFSDescriptor descriptor;
    std::vector<Slice> slices;
};

struct DfsReadRequest {
    std::string key;
    DistributedFSDescriptor descriptor;
    std::vector<Slice> slices;
};

/**
 * @brief Distributed filesystem and object storage backend.
 *
 * Uses a FileSystemAdapter for descriptor-based DFS reads and writes, or an
 * ObjectStorageAdapter for whole-object offload operations. Does not handle
 * eviction.
 */
class DistributedStorageBackend : public StorageBackendInterface {
   public:
    DistributedStorageBackend(
        const FileStorageConfig& file_storage_config,
        const DistributedStorageConfig& distributed_config,
        std::unique_ptr<FileSystemAdapter> fs_adapter);
    ~DistributedStorageBackend() override;

    // Exactly one adapter must be non-null.
    DistributedStorageBackend(
        const FileStorageConfig& file_storage_config,
        const DistributedStorageConfig& distributed_config,
        std::unique_ptr<FileSystemAdapter> fs_adapter,
        std::unique_ptr<ObjectStorageAdapter> object_storage_adapter);

    DistributedStorageMode GetStorageMode() const { return storage_mode_; }

    bool UsesObjectStorage() const {
        return storage_mode_ == DistributedStorageMode::kObjectStorage;
    }

    // Object mode only: does the configured adapter accept device Value ranges
    // without an HOST staging copy? False for file mode and for any adapter that
    // leaves the default capability bit alone. A true answer only means the write
    // path may skip its own staging; the adapter still validates each request and
    // refuses an unregistered range instead of staging it.
    bool AcceptsDevicePointers() const {
        return object_storage_adapter_ != nullptr &&
               object_storage_adapter_->AcceptsDevicePointers();
    }

    // Is this exact range carried by the adapter as device Value? See
    // ObjectStorageAdapter::AcceptsDeviceRange.
    bool AcceptsDeviceRange(const void* address, std::size_t length) const {
        return object_storage_adapter_ != nullptr &&
               object_storage_adapter_->AcceptsDeviceRange(address, length);
    }

    // Object mode only: drive the adapter's shutdown to a terminal state. This
    // is what lets an embedder release application device memory: a successful
    // Close means no in-flight DMA can still reference an arena. A retryable
    // error keeps every resource alive and must be retried. File mode has no
    // per-object close contract and reports NOT_SUPPORTED rather than pretending.
    tl::expected<void, ErrorCode> Close();

    tl::expected<void, ErrorCode> Init() override;

    tl::expected<int64_t, ErrorCode> BatchOffload(
        const std::unordered_map<std::string, std::vector<Slice>>& batch_object,
        std::function<ErrorCode(const std::vector<std::string>& keys,
                                std::vector<StorageObjectMetadata>& metadatas)>
            complete_handler,
        EvictionHandler eviction_handler = nullptr) override;

    std::vector<tl::expected<void, ErrorCode>> BatchWrite(
        const std::vector<DfsWriteRequest>& requests);

    std::vector<tl::expected<void, ErrorCode>> BatchRead(
        const std::vector<DfsReadRequest>& requests);

    // Object mode only: explicit physical cleanup entry. Callers that know a
    // key's metadata is gone (tests, maintenance tooling) use this to drop the
    // object from the object store as well.
    //
    // It is deliberately NOT called by Client::Remove/BatchRemove: doing the
    // physical delete inside those would change their public failure semantics
    // -- a retried removal would see the Master answer OBJECT_NOT_FOUND and skip
    // the physical half, and the delete would have to run before the local
    // hot-cache invalidation. File mode has no per-key physical object (the data
    // lives inside shard files) and reports NOT_SUPPORTED rather than pretending
    // to have deleted anything.
    tl::expected<void, ErrorCode> RemoveObject(const std::string& logical_key);

   private:
    // Object-storage branches of the two DFS batch entry points.
    //
    // Both keep the caller's positions rather than keying by name: a write batch
    // may legitimately name the same key twice, and a read batch may read one key
    // into two different destinations, so collapsing by key would merge requests
    // that are not the same request.
    std::vector<tl::expected<void, ErrorCode>> ObjectBatchWrite(
        const std::vector<DfsWriteRequest>& requests);
    std::vector<tl::expected<void, ErrorCode>> ObjectBatchRead(
        const std::vector<DfsReadRequest>& requests);

   public:

    // Key-only storage backend operations cannot safely address DFS objects;
    // callers must use BatchRead/BatchWrite with request-scoped descriptors.
    tl::expected<void, ErrorCode> BatchLoad(
        std::unordered_map<std::string, Slice>& batched_slices) override;

    tl::expected<bool, ErrorCode> IsExist(const std::string& key) override;

    tl::expected<bool, ErrorCode> IsEnableOffloading() override;

    tl::expected<void, ErrorCode> ScanMeta(
        const std::function<ErrorCode(
            const std::vector<std::string>& keys,
            std::vector<StorageObjectMetadata>& metadatas)>& handler) override;

   private:
    struct ShardFile {
        std::string path;
        int fd = -1;
        std::mutex mutex;
    };

    std::unique_ptr<FileSystemAdapter> fs_adapter_;
    std::unique_ptr<ObjectStorageAdapter> object_storage_adapter_;
    DistributedStorageConfig distributed_config_;
    std::string root_dir_;
    std::vector<std::unique_ptr<ShardFile>> shard_files_;
    DistributedStorageMode storage_mode_ = DistributedStorageMode::kFileSystem;
    bool initialized_ = false;
};

}  // namespace mooncake
