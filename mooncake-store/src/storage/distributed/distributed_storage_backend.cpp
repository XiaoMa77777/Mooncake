#include "storage/distributed/distributed_storage_backend.h"

#include <algorithm>
#include <filesystem>
#include <limits>
#include <sstream>

#include "environ.h"
#include "storage/distributed/dfs_global_allocator.h"
#include "types.h"
#include "utils.h"

namespace mooncake {

namespace {

bool IsDfsDescriptorRangeValid(const DistributedFSDescriptor& desc,
                               const DistributedStorageConfig& config) {
    if (config.alignment == 0 || desc.object_size == 0 ||
        desc.aligned_size < desc.object_size ||
        desc.offset % config.alignment != 0 ||
        desc.aligned_size % config.alignment != 0) {
        return false;
    }
    if (desc.offset > config.shard_capacity ||
        desc.aligned_size > config.shard_capacity - desc.offset) {
        return false;
    }

    constexpr uint64_t kMaxFileOffset =
        static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
    return desc.offset <= kMaxFileOffset &&
           desc.aligned_size <= kMaxFileOffset - desc.offset;
}

}  // namespace

bool DistributedStorageConfig::Validate() const {
    if (fsdir.empty()) {
        LOG(ERROR) << "DistributedStorageConfig: fsdir is empty";
        return false;
    }
    if (!std::filesystem::path(fsdir).is_absolute()) {
        LOG(ERROR)
            << "DistributedStorageConfig: fsdir must be an absolute path: "
            << fsdir;
        return false;
    }
    if (fs_adapter_type != "hf3fs" && fs_adapter_type != "posix" &&
        fs_adapter_type != "object_storage") {
        LOG(ERROR) << "DistributedStorageConfig: unsupported fs_adapter_type: "
                   << fs_adapter_type;
        return false;
    }
    if (fs_adapter_type == "object_storage") {
        // Object mode has no shard file to size or align, so the filesystem
        // checks below do not describe it. What it does need is its own identity
        // and the SDK configuration its transport is built from -- without those
        // there is no object namespace to address and no way to reach it.
        if (object_cache_instance.empty()) {
            LOG(ERROR) << "DistributedStorageConfig: object_storage requires "
                          "object_cache_instance";
            return false;
        }
        if (object_epoch == 0) {
            LOG(ERROR) << "DistributedStorageConfig: object_storage requires a "
                          "non-zero object_epoch";
            return false;
        }
        if (object_layout_version != 1) {
            LOG(ERROR) << "DistributedStorageConfig: unsupported "
                          "object_layout_version: "
                       << object_layout_version;
            return false;
        }
        if (kvclient_sdk_config_path.empty()) {
            LOG(ERROR) << "DistributedStorageConfig: object_storage requires "
                          "kvclient_sdk_config_path";
            return false;
        }
        if (!single_tenant) {
            LOG(ERROR) << "DistributedStorageConfig: Currently, DFS requires "
                          "single_tenant=true";
            return false;
        }
        return true;
    }
    if (shard_count <= 0) {
        LOG(ERROR) << "DistributedStorageConfig: shard_count must > 0";
        return false;
    }
    if (shard_capacity == 0) {
        LOG(ERROR) << "DistributedStorageConfig: shard_capacity must > 0";
        return false;
    }
    if (alignment == 0 || (alignment & (alignment - 1)) != 0) {
        LOG(ERROR) << "DistributedStorageConfig: alignment must be power of 2";
        return false;
    }
    if (shard_capacity % alignment != 0) {
        LOG(ERROR) << "DistributedStorageConfig: shard_capacity must align";
        return false;
    }
    if (!single_tenant) {
        LOG(ERROR) << "DistributedStorageConfig: Currently, DFS requires "
                      "single_tenant=true";
        return false;
    }
    return true;
}

bool DistributedStorageConfig::ValidateForAllocator() const {
    // Checked before Validate(), so the refusal says what it means. The Master's
    // allocator is a filesystem allocator; object mode is a *client*
    // configuration. Accepting it here is what would let
    // `DfsGlobalAllocator::Init` be handed a config it cannot build an adapter
    // for -- and then dereference the null one. The deployment shape is
    // therefore Master on hf3fs/posix, client on object_storage.
    if (fs_adapter_type == "object_storage") {
        LOG(ERROR) << "DistributedStorageConfig: object_storage is a client "
                      "configuration; the Master allocator needs hf3fs or posix";
        return false;
    }
    if (!Validate()) return false;

    if (eviction_low_watermark < 0.0 || eviction_low_watermark > 1.0 ||
        eviction_high_watermark < 0.0 || eviction_high_watermark > 1.0 ||
        eviction_low_watermark >= eviction_high_watermark) {
        LOG(ERROR) << "DistributedStorageConfig: eviction watermarks must "
                      "satisfy 0 <= low < high <= 1, low="
                   << eviction_low_watermark
                   << ", high=" << eviction_high_watermark;
        return false;
    }
    if (deferred_free_duration.count() < 0) {
        LOG(ERROR) << "DistributedStorageConfig: deferred_free_duration must "
                      "be non-negative, seconds="
                   << deferred_free_duration.count();
        return false;
    }
    if (eviction_enabled && eviction_check_interval.count() <= 0) {
        LOG(ERROR) << "DistributedStorageConfig: eviction_check_interval must "
                      "be positive when eviction is enabled, seconds="
                   << eviction_check_interval.count();
        return false;
    }
    return true;
}

DistributedStorageConfig DistributedStorageConfig::FromEnvironment() {
    DistributedStorageConfig config;
    config.fsdir = Environ::GetString(
        "MOONCAKE_DFS_ROOT_DIR",
        Environ::GetString("MOONCAKE_DISTRIBUTED_ROOT_DIR", config.fsdir));
    if (!std::filesystem::path(config.fsdir).is_absolute()) {
        config.fsdir = std::filesystem::absolute(config.fsdir).string();
    }
    config.fs_adapter_type =
        Environ::GetString("MOONCAKE_DFS_FS_ADAPTER",
                           Environ::GetString("MOONCAKE_DISTRIBUTED_FS_TYPE",
                                              config.fs_adapter_type));
    config.enable_health_check =
        Environ::GetBool("MOONCAKE_DISTRIBUTED_HEALTH_CHECK", false);
    config.object_cache_instance = Environ::GetString(
        "MOONCAKE_DFS_OBJECT_CACHE_INSTANCE", config.object_cache_instance);
    config.object_epoch = Environ::GetUInt64("MOONCAKE_DFS_OBJECT_EPOCH",
                                             config.object_epoch);
    config.object_layout_version = static_cast<uint32_t>(Environ::GetUInt64(
        "MOONCAKE_DFS_OBJECT_LAYOUT_VERSION", config.object_layout_version));
    config.object_duplicate_window_keys = static_cast<size_t>(Environ::GetUInt64(
        "MOONCAKE_DFS_OBJECT_DUPLICATE_WINDOW_KEYS",
        config.object_duplicate_window_keys));
    config.object_duplicate_admission = Environ::GetString(
        "MOONCAKE_DFS_OBJECT_DUPLICATE_ADMISSION",
        config.object_duplicate_admission);
    config.object_scratch_budget_bytes = static_cast<size_t>(Environ::GetUInt64(
        "MOONCAKE_DFS_OBJECT_SCRATCH_BUDGET_BYTES",
        config.object_scratch_budget_bytes));
    config.kvclient_sdk_config_path = Environ::GetString(
        "MOONCAKE_DFS_KVCLIENT_SDK_CONFIG", config.kvclient_sdk_config_path);
    config.kvclient_timeout_ms = static_cast<uint32_t>(Environ::GetUInt64(
        "MOONCAKE_DFS_KVCLIENT_TIMEOUT_MS", config.kvclient_timeout_ms));
    config.shard_count =
        Environ::GetInt("MOONCAKE_DFS_SHARD_COUNT", config.shard_count);
    config.shard_capacity = Environ::GetUInt64("MOONCAKE_DFS_SHARD_CAPACITY",
                                               config.shard_capacity);
    config.alignment =
        Environ::GetUInt64("MOONCAKE_DFS_ALIGNMENT", config.alignment);
    config.single_tenant =
        Environ::GetBool("MOONCAKE_DFS_SINGLE_TENANT", config.single_tenant);
    config.eviction_enabled = Environ::GetBool("MOONCAKE_DFS_EVICTION_ENABLED",
                                               config.eviction_enabled);
    config.eviction_high_watermark = Environ::GetDouble(
        "MOONCAKE_DFS_EVICTION_HIGH_WATERMARK", config.eviction_high_watermark);
    config.eviction_low_watermark = Environ::GetDouble(
        "MOONCAKE_DFS_EVICTION_LOW_WATERMARK", config.eviction_low_watermark);
    config.deferred_free_duration = std::chrono::seconds(Environ::GetInt(
        "MOONCAKE_DFS_DEFERRED_FREE_SECONDS",
        static_cast<int>(config.deferred_free_duration.count())));
    config.eviction_check_interval = std::chrono::seconds(Environ::GetInt(
        "MOONCAKE_DFS_EVICTION_CHECK_INTERVAL",
        static_cast<int>(config.eviction_check_interval.count())));
    return config;
}

std::string DistributedStorageConfig::FormatStr() const {
    std::ostringstream oss;
    oss << "fsdir=" << fsdir << ", fs_adapter_type=" << fs_adapter_type
        << ", enable_health_check=" << enable_health_check
        << ", shard_count=" << shard_count
        << ", shard_capacity=" << shard_capacity << ", alignment=" << alignment
        << ", single_tenant=" << single_tenant
        << ", eviction_enabled=" << eviction_enabled
        << ", eviction_high_watermark=" << eviction_high_watermark
        << ", eviction_low_watermark=" << eviction_low_watermark
        << ", deferred_free_seconds=" << deferred_free_duration.count()
        << ", eviction_check_interval_seconds="
        << eviction_check_interval.count();
    return oss.str();
}

DistributedStorageBackend::DistributedStorageBackend(
    const FileStorageConfig& file_storage_config,
    const DistributedStorageConfig& distributed_config,
    std::unique_ptr<FileSystemAdapter> fs_adapter)
    : DistributedStorageBackend(file_storage_config, distributed_config,
                                std::move(fs_adapter), nullptr) {}

DistributedStorageBackend::DistributedStorageBackend(
    const FileStorageConfig& file_storage_config,
    const DistributedStorageConfig& distributed_config,
    std::unique_ptr<FileSystemAdapter> fs_adapter,
    std::unique_ptr<ObjectStorageAdapter> object_storage_adapter)
    : StorageBackendInterface(file_storage_config),
      fs_adapter_(std::move(fs_adapter)),
      object_storage_adapter_(std::move(object_storage_adapter)),
      distributed_config_(distributed_config),
      root_dir_(distributed_config.fsdir) {
    CHECK((fs_adapter_ != nullptr) != (object_storage_adapter_ != nullptr))
        << "DistributedStorageBackend: exactly one I/O adapter is required";
    if (object_storage_adapter_) {
        storage_mode_ = DistributedStorageMode::kObjectStorage;
    }
}

DistributedStorageBackend::~DistributedStorageBackend() {
    for (auto& shard : shard_files_) {
        if (shard && shard->fd >= 0 && fs_adapter_) {
            fs_adapter_->CloseFile(shard->fd);
            shard->fd = -1;
        }
    }
    if (fs_adapter_) fs_adapter_->Shutdown();
}

tl::expected<void, ErrorCode> DistributedStorageBackend::Close() {
    if (object_storage_adapter_ == nullptr) {
        // File mode keeps its shard descriptors until destruction; there is no
        // "no DMA can still touch your buffer" claim to make here.
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    }
    return object_storage_adapter_->Close();
}

tl::expected<void, ErrorCode> DistributedStorageBackend::Init() {
    if (initialized_) {
        LOG(WARNING) << "DistributedStorageBackend is already initialized";
        return {};
    }

    if (UsesObjectStorage()) {
        auto init_result = object_storage_adapter_->Init();
        if (!init_result) return init_result;
        initialized_ = true;
        LOG(INFO) << "DistributedStorageBackend initialized, object adapter="
                  << object_storage_adapter_->GetName();
        return {};
    }

    std::error_code ec;
    std::filesystem::create_directories(root_dir_, ec);
    if (ec) {
        LOG(ERROR) << "Failed to create DFS root directory " << root_dir_
                   << ": " << ec.message();
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }

    auto init_result = fs_adapter_->Init(root_dir_);
    if (!init_result) return init_result;

    shard_files_.reserve(distributed_config_.shard_count);
    for (int i = 0; i < distributed_config_.shard_count; ++i) {
        std::string path = root_dir_ + "/dfs_shard_" +
                           DfsGlobalAllocator::FormatShardIdx(
                               i, distributed_config_.shard_count) +
                           ".data";
        auto fd_result = fs_adapter_->OpenFile(path);
        if (!fd_result) {
            LOG(ERROR) << "Failed to open DFS shard " << path << ": "
                       << fd_result.error();
            return tl::make_unexpected(fd_result.error());
        }
        auto shard = std::make_unique<ShardFile>();
        shard->path = std::move(path);
        shard->fd = *fd_result;
        shard_files_.push_back(std::move(shard));
    }

    initialized_ = true;
    return {};
}

tl::expected<int64_t, ErrorCode> DistributedStorageBackend::BatchOffload(
    const std::unordered_map<std::string, std::vector<Slice>>& batch_object,
    std::function<ErrorCode(const std::vector<std::string>& keys,
                            std::vector<StorageObjectMetadata>& metadatas)>
        complete_handler,
    EvictionHandler eviction_handler) {
    if (!UsesObjectStorage()) {
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    }
    if (!initialized_) {
        LOG(ERROR) << "DistributedStorageBackend is not initialized";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    if (eviction_handler) {
        LOG_FIRST_N(WARNING, 1)
            << "DistributedStorageBackend does not support eviction, "
               "eviction_handler ignored";
    }

    std::vector<std::string> success_keys;
    std::vector<StorageObjectMetadata> success_metas;
    for (const auto& [key, slices] : batch_object) {
        if (slices.size() >
            static_cast<size_t>(std::numeric_limits<int>::max())) {
            LOG(WARNING) << "Failed to offload key " << key
                         << ": slice count exceeds INT_MAX";
            continue;
        }
        const int iovcnt = static_cast<int>(slices.size());

        std::vector<iovec> iovs;
        iovs.reserve(slices.size());
        size_t total_size = 0;
        bool total_size_overflow = false;
        for (const auto& slice : slices) {
            if (slice.size > std::numeric_limits<size_t>::max() - total_size) {
                total_size_overflow = true;
                break;
            }
            iovs.push_back({slice.ptr, slice.size});
            total_size += slice.size;
        }
        if (total_size_overflow) {
            LOG(WARNING) << "Failed to offload key " << key
                         << ": total slice size overflows size_t";
            continue;
        }

        auto result = object_storage_adapter_->PutV(key, iovs.data(), iovcnt);
        if (!result) {
            LOG(WARNING) << "Failed to offload key " << key << ": "
                         << static_cast<int>(result.error());
            continue;
        }

        success_keys.push_back(key);
        success_metas.emplace_back(-1, 0, static_cast<int64_t>(key.size()),
                                   static_cast<int64_t>(total_size), "");
    }

    if (complete_handler && !success_keys.empty()) {
        auto err = complete_handler(success_keys, success_metas);
        if (err != ErrorCode::OK) {
            return tl::make_unexpected(err);
        }
    }
    return static_cast<int64_t>(success_keys.size());
}

std::vector<tl::expected<void, ErrorCode>>
DistributedStorageBackend::BatchWrite(
    const std::vector<DfsWriteRequest>& requests) {
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(requests.size());

    if (UsesObjectStorage()) {
        return ObjectBatchWrite(requests);
    }
    if (!initialized_) {
        LOG(ERROR) << "DistributedStorageBackend is not initialized";
        results.assign(requests.size(),
                       tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE));
        return results;
    }

    for (const auto& request : requests) {
        const auto& desc = request.descriptor;
        if (desc.shard_idx < 0 ||
            desc.shard_idx >= static_cast<int>(shard_files_.size())) {
            LOG(ERROR) << "Invalid DFS shard_idx " << desc.shard_idx
                       << " for key " << request.key;
            results.emplace_back(
                tl::make_unexpected(ErrorCode::INVALID_PARAMS));
            continue;
        }

        auto& shard = *shard_files_[desc.shard_idx];
        if (desc.file_path != shard.path) {
            LOG(ERROR) << "DFS path mismatch for key " << request.key
                       << ", descriptor=" << desc.file_path
                       << ", configured=" << shard.path;
            results.emplace_back(
                tl::make_unexpected(ErrorCode::INVALID_PARAMS));
            continue;
        }
        if (!IsDfsDescriptorRangeValid(desc, distributed_config_)) {
            LOG(ERROR) << "Invalid DFS descriptor range for key " << request.key
                       << ", offset=" << desc.offset
                       << ", object_size=" << desc.object_size
                       << ", aligned_size=" << desc.aligned_size
                       << ", shard_capacity="
                       << distributed_config_.shard_capacity;
            results.emplace_back(
                tl::make_unexpected(ErrorCode::INVALID_PARAMS));
            continue;
        }

        std::vector<iovec> iovs;
        iovs.reserve(request.slices.size());
        uint64_t total_size = 0;
        bool invalid = false;
        for (const auto& slice : request.slices) {
            if ((!slice.ptr && slice.size > 0) ||
                slice.size >
                    std::numeric_limits<uint64_t>::max() - total_size) {
                invalid = true;
                break;
            }
            total_size += slice.size;
            iovs.push_back({slice.ptr, slice.size});
        }
        if (invalid || total_size != desc.object_size) {
            LOG(WARNING) << "Invalid DFS write request for key " << request.key
                         << ", expected=" << desc.object_size
                         << ", actual=" << total_size;
            results.emplace_back(
                tl::make_unexpected(ErrorCode::INVALID_PARAMS));
            continue;
        }

        std::lock_guard lock(shard.mutex);
        auto write_result =
            fs_adapter_->WriteAt(shard.fd, iovs.data(), iovs.size(),
                                 static_cast<int64_t>(desc.offset));
        if (!write_result) {
            LOG(WARNING) << "DFS write failed for key " << request.key
                         << ", error=" << write_result.error();
            results.emplace_back(tl::make_unexpected(write_result.error()));
            continue;
        }
        if (*write_result != total_size) {
            LOG(WARNING) << "DFS short write for key " << request.key
                         << ", expected=" << total_size
                         << ", actual=" << *write_result;
            results.emplace_back(
                tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL));
            continue;
        }
        results.emplace_back();
    }
    return results;
}

tl::expected<void, ErrorCode> DistributedStorageBackend::RemoveObject(
    const std::string& logical_key) {
    if (!initialized_ || object_storage_adapter_ == nullptr) {
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    }
    return object_storage_adapter_->Delete(logical_key);
}

std::vector<tl::expected<void, ErrorCode>>
DistributedStorageBackend::ObjectBatchWrite(
    const std::vector<DfsWriteRequest>& requests) {
    std::vector<tl::expected<void, ErrorCode>> results(requests.size());
    if (!initialized_ || object_storage_adapter_ == nullptr) {
        for (auto& result : results) {
            result = tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
        }
        return results;
    }

    // A write batch that names the same key twice is refused *at every position
    // that names it*, not just at the later one. Submitting both would leave the
    // outcome to the adapter -- which may well accept both, keep whichever it
    // wrote last, and report success twice for one object, which is not the same
    // answer as either write.
    std::unordered_map<std::string, int> key_counts;
    key_counts.reserve(requests.size() * 2);
    for (const auto& request : requests) {
        ++key_counts[request.key];
    }

    // Positions, not keys: the surviving requests are mapped back by index, so
    // several distinct requests stay distinct all the way down.
    std::vector<ObjectPutRequest> batch;
    std::vector<std::size_t> position;
    batch.reserve(requests.size());
    position.reserve(requests.size());

    for (std::size_t i = 0; i < requests.size(); ++i) {
        const auto& request = requests[i];
        if (key_counts[request.key] > 1) {
            results[i] = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }
        const std::uint64_t expected = request.descriptor.object_size;
        std::vector<iovec> iovs;
        iovs.reserve(request.slices.size());
        std::size_t total = 0;
        bool usable = expected != 0;
        for (const auto& slice : request.slices) {
            if (slice.size == 0) {
                continue;  // an empty region contributes nothing and must not fail
            }
            if (slice.ptr == nullptr ||
                slice.size > std::numeric_limits<std::size_t>::max() - total) {
                usable = false;
                break;
            }
            total += slice.size;
            iovec iov{const_cast<void*>(slice.ptr), slice.size};
            iovs.push_back(iov);
        }
        // The regions must describe exactly the object. Anything else is refused
        // before any I/O, and the refusal stays with this item.
        if (!usable || total != expected) {
            results[i] = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }
        batch.push_back(ObjectPutRequest{request.key, std::move(iovs)});
        position.push_back(i);
    }

    if (!batch.empty()) {
        auto batch_results = object_storage_adapter_->PutVBatch(batch);
        if (batch_results.size() != batch.size()) {
            // A result count that does not match its input cannot be indexed
            // safely; fail the whole submission rather than read past the end.
            LOG(ERROR) << "object adapter returned " << batch_results.size()
                       << " results for " << batch.size() << " writes";
            for (std::size_t n = 0; n < batch.size(); ++n) {
                results[position[n]] = tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }
            return results;
        }
        for (std::size_t n = 0; n < batch.size(); ++n) {
            results[position[n]] = batch_results[n];
        }
    }
    return results;
}

std::vector<tl::expected<void, ErrorCode>>
DistributedStorageBackend::ObjectBatchRead(
    const std::vector<DfsReadRequest>& requests) {
    std::vector<tl::expected<void, ErrorCode>> results(requests.size());
    if (!initialized_ || object_storage_adapter_ == nullptr) {
        for (auto& result : results) {
            result = tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
        }
        return results;
    }

    std::vector<ObjectGetRequest> batch;
    std::vector<std::size_t> position;
    batch.reserve(requests.size());
    position.reserve(requests.size());

    for (std::size_t i = 0; i < requests.size(); ++i) {
        const auto& request = requests[i];
        const std::uint64_t expected = request.descriptor.object_size;
        std::vector<iovec> iovs;
        iovs.reserve(request.slices.size());
        std::size_t remaining = static_cast<std::size_t>(expected);
        bool usable = expected != 0 && expected <= std::numeric_limits<std::size_t>::max();
        if (usable) {
            for (const auto& slice : request.slices) {
                if (remaining == 0) {
                    break;
                }
                if (slice.size == 0) {
                    continue;
                }
                if (slice.ptr == nullptr) {
                    usable = false;
                    break;
                }
                // The caller may offer more capacity than the object needs -- a
                // tail guard. That is legal and must not become a rejection, so
                // the regions are clipped to exactly `object_size`; the guard is
                // left untouched.
                const std::size_t take =
                    std::min<std::size_t>(slice.size, remaining);
                iovec iov{slice.ptr, take};
                iovs.push_back(iov);
                remaining -= take;
            }
        }
        // Insufficient capacity is a refusal, not a short read: the object does
        // not fit, and pretending otherwise would deliver part of it.
        if (!usable || remaining != 0) {
            results[i] = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }
        batch.push_back(ObjectGetRequest{request.key, std::move(iovs),
                                         static_cast<std::size_t>(expected)});
        position.push_back(i);
    }

    if (!batch.empty()) {
        auto batch_results = object_storage_adapter_->GetBatch(batch);
        if (batch_results.size() != batch.size()) {
            LOG(ERROR) << "object adapter returned " << batch_results.size()
                       << " results for " << batch.size() << " reads";
            for (std::size_t n = 0; n < batch.size(); ++n) {
                results[position[n]] = tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }
            return results;
        }
        for (std::size_t n = 0; n < batch.size(); ++n) {
            const std::size_t index = position[n];
            if (!batch_results[n]) {
                results[index] = tl::make_unexpected(batch_results[n].error());
                continue;
            }
            // The reported length must be the object's real length. A provider
            // that echoed the caller's capacity would pass this only by accident.
            if (*batch_results[n] !=
                static_cast<std::size_t>(requests[index].descriptor.object_size)) {
                results[index] = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
        }
    }
    return results;
}

std::vector<tl::expected<void, ErrorCode>> DistributedStorageBackend::BatchRead(
    const std::vector<DfsReadRequest>& requests) {
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(requests.size());

    if (UsesObjectStorage()) {
        return ObjectBatchRead(requests);
    }
    if (!initialized_) {
        LOG(ERROR) << "DistributedStorageBackend is not initialized";
        results.assign(requests.size(),
                       tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE));
        return results;
    }

    for (const auto& request : requests) {
        const auto& desc = request.descriptor;
        if (desc.shard_idx < 0 ||
            desc.shard_idx >= static_cast<int>(shard_files_.size())) {
            LOG(ERROR) << "Invalid DFS shard_idx " << desc.shard_idx
                       << " for key " << request.key;
            results.emplace_back(
                tl::make_unexpected(ErrorCode::INVALID_PARAMS));
            continue;
        }

        auto& shard = *shard_files_[desc.shard_idx];
        if (desc.file_path != shard.path) {
            LOG(ERROR) << "DFS path mismatch for key " << request.key
                       << ", descriptor=" << desc.file_path
                       << ", configured=" << shard.path;
            results.emplace_back(
                tl::make_unexpected(ErrorCode::INVALID_PARAMS));
            continue;
        }
        if (!IsDfsDescriptorRangeValid(desc, distributed_config_)) {
            LOG(ERROR) << "Invalid DFS descriptor range for key " << request.key
                       << ", offset=" << desc.offset
                       << ", object_size=" << desc.object_size
                       << ", aligned_size=" << desc.aligned_size
                       << ", shard_capacity="
                       << distributed_config_.shard_capacity;
            results.emplace_back(
                tl::make_unexpected(ErrorCode::INVALID_PARAMS));
            continue;
        }
        if (desc.object_size > std::numeric_limits<size_t>::max() ||
            request.slices.size() >
                static_cast<size_t>(std::numeric_limits<int>::max())) {
            results.emplace_back(
                tl::make_unexpected(ErrorCode::INVALID_PARAMS));
            continue;
        }

        std::vector<iovec> iovs;
        iovs.reserve(request.slices.size());
        size_t remaining = static_cast<size_t>(desc.object_size);
        bool invalid = false;
        for (const auto& slice : request.slices) {
            if (!slice.ptr && slice.size > 0) {
                invalid = true;
                break;
            }
            if (remaining == 0 || slice.size == 0) {
                continue;
            }
            const size_t read_size = std::min(slice.size, remaining);
            iovs.push_back({slice.ptr, read_size});
            remaining -= read_size;
        }
        if (invalid || remaining != 0) {
            LOG(WARNING) << "Invalid DFS read request for key " << request.key
                         << ", expected capacity at least=" << desc.object_size;
            results.emplace_back(
                tl::make_unexpected(ErrorCode::INVALID_PARAMS));
            continue;
        }

        std::lock_guard lock(shard.mutex);
        auto read_result = fs_adapter_->ReadAt(
            shard.fd, iovs.data(), static_cast<int>(iovs.size()),
            static_cast<int64_t>(desc.offset));
        if (!read_result) {
            LOG(WARNING) << "DFS read failed for key " << request.key
                         << ", error=" << read_result.error();
            results.emplace_back(tl::make_unexpected(read_result.error()));
            continue;
        }
        if (*read_result != desc.object_size) {
            LOG(WARNING) << "DFS short read for key " << request.key
                         << ", expected=" << desc.object_size
                         << ", actual=" << *read_result;
            results.emplace_back(
                tl::make_unexpected(ErrorCode::FILE_READ_FAIL));
            continue;
        }
        results.emplace_back();
    }
    return results;
}

tl::expected<void, ErrorCode> DistributedStorageBackend::BatchLoad(
    std::unordered_map<std::string, Slice>& batched_slices) {
    if (!UsesObjectStorage()) {
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    }
    if (!initialized_) {
        LOG(ERROR) << "DistributedStorageBackend is not initialized";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    for (auto& [key, slice] : batched_slices) {
        auto result = object_storage_adapter_->Get(key, slice.ptr, slice.size);
        if (!result) {
            return tl::make_unexpected(result.error());
        }
        if (*result != slice.size) {
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
    }
    return {};
}

tl::expected<bool, ErrorCode> DistributedStorageBackend::IsExist(
    const std::string& key) {
    if (!UsesObjectStorage()) {
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    }
    if (!initialized_) {
        LOG(ERROR) << "DistributedStorageBackend is not initialized";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return object_storage_adapter_->Exists(key);
}

tl::expected<bool, ErrorCode> DistributedStorageBackend::IsEnableOffloading() {
    return UsesObjectStorage();
}

tl::expected<void, ErrorCode> DistributedStorageBackend::ScanMeta(
    const std::function<
        ErrorCode(const std::vector<std::string>& keys,
                  std::vector<StorageObjectMetadata>& metadatas)>& handler) {
    if (!UsesObjectStorage()) {
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    }
    if (!initialized_) {
        LOG(ERROR) << "DistributedStorageBackend is not initialized";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    std::vector<std::string> batch_keys;
    std::vector<StorageObjectMetadata> batch_metas;
    const size_t batch_limit = static_cast<size_t>(std::max<int64_t>(
        1, file_storage_config_.scanmeta_iterator_keys_limit));

    auto key_infos = object_storage_adapter_->ListKeys();
    if (!key_infos) {
        LOG(ERROR) << "Failed to list keys from object storage adapter: "
                   << static_cast<int>(key_infos.error());
        return tl::make_unexpected(key_infos.error());
    }

    for (const auto& info : *key_infos) {
        batch_keys.push_back(info.logical_key);
        batch_metas.emplace_back(-1, 0,
                                 static_cast<int64_t>(info.logical_key.size()),
                                 static_cast<int64_t>(info.size), "");
        if (batch_keys.size() >= batch_limit) {
            auto err = handler(batch_keys, batch_metas);
            if (err != ErrorCode::OK) return tl::make_unexpected(err);
            batch_keys.clear();
            batch_metas.clear();
        }
    }
    if (!batch_keys.empty()) {
        auto err = handler(batch_keys, batch_metas);
        if (err != ErrorCode::OK) return tl::make_unexpected(err);
    }
    return {};
}

}  // namespace mooncake
