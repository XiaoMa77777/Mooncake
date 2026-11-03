// Copyright (c) 2026 Hygon Information Technology Co., Ltd.
// SPDX-License-Identifier: Apache-2.0

#include "kvclient_backend.h"

#if defined(MOONCAKE_WITH_KVCLIENT)

#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <utility>

#include <glog/logging.h>
#include <kvclient/mooncake/storage_backend.hpp>
#include <kvclient/mooncake/transport.hpp>

namespace mooncake {

namespace {

std::string BuildIndexPath(const FileStorageConfig& config,
                           const KvclientBackendConfig& kvclient_config) {
    if (!kvclient_config.index_path.empty()) {
        return kvclient_config.index_path;
    }
    const std::filesystem::path directory =
        kvclient_config.index_directory.empty()
            ? config.storage_filepath
            : kvclient_config.index_directory;
    if (directory.empty() || !directory.is_absolute()) {
        throw std::invalid_argument("kvclient index directory must be absolute");
    }
    // 固定身份用于重启恢复；并发客户端应配置不同路径。底层索引独占锁会
    // 拒绝共享同一文件，父目录的创建和持久化也由底层索引负责。
    return (directory / "client.index").string();
}

// 有界批并发参数。取值必须由部署显式给出（环境变量），非法值拒绝启动而不是
// 静默退回默认；启动日志逐项打印实际生效值，测量时不必猜配置。
uint64_t ReadBoundedOption(const char* name, uint64_t fallback, uint64_t low,
                           uint64_t high) {
    const char* text = std::getenv(name);
    if (text == nullptr || *text == '\0') {
        return fallback;
    }
    char* end = nullptr;
    const unsigned long long value = std::strtoull(text, &end, 10);
    if (end == text || *end != '\0' || value < low || value > high) {
        throw std::invalid_argument(std::string(name) +
                                    " must be an integer in [" +
                                    std::to_string(low) + ", " +
                                    std::to_string(high) + "]");
    }
    return value;
}

}  // namespace

KvclientStorageBackend::KvclientStorageBackend(
    const FileStorageConfig& config,
    const KvclientBackendConfig& kvclient_config)
    : StorageBackendInterface(config) {
    // 直接构造与工厂构造执行相同校验，避免未经校验的整数截断或设备路径。
    if (!kvclient_config.Validate()) {
        throw std::invalid_argument("Invalid KvclientBackendConfig");
    }
    aistor::SdkTransportOptions transport_options;
    transport_options.sdkConfigPath = kvclient_config.sdk_config_path;
    transport_options.timeoutMs =
        static_cast<std::uint32_t>(kvclient_config.timeout_ms);
    // 0 个载荷槽是显式 QD=1 对照；其余取值决定注册内存上限，所以上限保守。
    transport_options.hostPayloadSlots = static_cast<std::uint32_t>(
        ReadBoundedOption("MOONCAKE_OFFLOAD_KVCLIENT_PAYLOAD_SLOTS", 32, 0, 256));
    transport_options.hostSmallSlots = static_cast<std::uint32_t>(
        ReadBoundedOption("MOONCAKE_OFFLOAD_KVCLIENT_SMALL_SLOTS", 16, 0, 1024));
    transport_options.maxInFlightBytes = ReadBoundedOption(
        "MOONCAKE_OFFLOAD_KVCLIENT_MAX_INFLIGHT_BYTES", 256ULL * 1024ULL * 1024ULL,
        1024ULL * 1024ULL, 1024ULL * 1024ULL * 1024ULL);
    // 载荷槽容量跟随单值上限：小对象不必按 4 MiB 协议上限预留槽位，注册内存
    // 于是随工作负载而不是随协议上限增长。单值格式下一个对象就是一条记录，
    // 所以槽只需装得下 max_object_bytes。
    transport_options.payloadSlotBytes = static_cast<std::uint64_t>(
        ReadBoundedOption("MOONCAKE_OFFLOAD_KVCLIENT_PAYLOAD_SLOT_BYTES",
                          kvclient_config.max_object_bytes, 4096,
                          4ULL * 1024ULL * 1024ULL));
    transport_options.requestTimeoutMs = static_cast<std::uint32_t>(
        ReadBoundedOption("MOONCAKE_OFFLOAD_KVCLIENT_REQUEST_TIMEOUT_MS",
                          kvclient_config.timeout_ms, 1, 600000));
    LOG(INFO) << "kvclient batch transport: payload_slots="
              << transport_options.hostPayloadSlots
              << " payload_slot_bytes=" << transport_options.payloadSlotBytes
              << " small_slots=" << transport_options.hostSmallSlots
              << " max_in_flight_bytes=" << transport_options.maxInFlightBytes
              << " request_timeout_ms=" << transport_options.requestTimeoutMs;
    transport_ =
        std::make_shared<aistor::SdkTransport>(std::move(transport_options));

    aistor::BackendOptions backend_options;
    backend_options.prefix = kvclient_config.prefix;
    backend_options.layout = kvclient_config.layout;
    // FileStorage 已将设备数据转为 HOST 数据，当前封装不注册设备 arena。
    backend_options.devicePayloads = false;
    // 单值上限：配置值必须落在 (0, 4 MiB]，且不高于 SDK 协商能力。后者要到
    // transport 初始化后才可读，因此由后端 Init 再校验一次；两者都通过时
    // 有效上限就等于三者中的最小值。
    backend_options.maxObjectBytes =
        static_cast<std::uint64_t>(kvclient_config.max_object_bytes);
    backend_options.indexPath = BuildIndexPath(config, kvclient_config);
    // 启动记录实际生效的身份与上限，便于在多进程布局中核对一致性。
    LOG(INFO) << "kvclient backend: prefix=" << backend_options.prefix
              << " layout=" << backend_options.layout
              << " max_object_bytes=" << backend_options.maxObjectBytes
              << " index=" << backend_options.indexPath;
    // The index barrier costs two fsyncs per batch regardless of how many
    // objects the batch holds, so the per-object index cost is the batch's
    // fixed cost divided by its size. Expose it so the trade-off (a larger
    // batch means a wider window of staged-but-uncommitted objects) can be
    // measured rather than assumed.
    if (const char* batch = std::getenv("MOONCAKE_OFFLOAD_KVCLIENT_INDEX_BATCH_KEYS")) {
        const auto value = std::strtoul(batch, nullptr, 10);
        if (value > 0) backend_options.indexBatchKeys = value;
    }
    backend_options.indexBatchBytes = ReadBoundedOption(
        "MOONCAKE_OFFLOAD_KVCLIENT_INDEX_BATCH_BYTES", backend_options.indexBatchBytes,
        1024ULL * 1024ULL, 256ULL * 1024ULL * 1024ULL);
    LOG(INFO) << "kvclient index batching: batch_keys=" << backend_options.indexBatchKeys
              << " batch_bytes=" << backend_options.indexBatchBytes
              << " max_object_bytes=" << backend_options.maxObjectBytes;

    backend_ = std::make_unique<aistor::KvclientStorageBackend>(
        config, std::move(backend_options), transport_);
}

KvclientStorageBackend::~KvclientStorageBackend() {
    // 先销毁后端，随后关闭传输。关闭未完成时由 SdkTransport 析构中的安全
    // 保留机制保护仍注册的资源，不强制释放可能仍被 DMA 使用的缓冲区。
    backend_.reset();
    if (!transport_) {
        return;
    }
    try {
        const aistor::IoResult result = transport_->Close();
        if (!result.ok()) {
            LOG(WARNING) << "kvclient transport close did not complete: "
                         << result.message;
        }
    } catch (...) {
        LOG(WARNING) << "kvclient transport close raised an exception";
    }
}

tl::expected<void, ErrorCode> KvclientStorageBackend::Init() {
    return backend_->Init();
}

tl::expected<int64_t, ErrorCode> KvclientStorageBackend::BatchOffload(
    const std::unordered_map<std::string, std::vector<Slice>>& batch_object,
    std::function<ErrorCode(const std::vector<std::string>& keys,
                            std::vector<StorageObjectMetadata>& metadatas)>
        complete_handler,
    EvictionHandler eviction_handler) {
    return backend_->BatchOffload(batch_object, std::move(complete_handler),
                                  std::move(eviction_handler));
}

tl::expected<void, ErrorCode> KvclientStorageBackend::BatchLoad(
    std::unordered_map<std::string, Slice>& batched_slices) {
    return backend_->BatchLoad(batched_slices);
}

tl::expected<bool, ErrorCode> KvclientStorageBackend::IsExist(
    const std::string& key) {
    return backend_->IsExist(key);
}

tl::expected<bool, ErrorCode> KvclientStorageBackend::IsEnableOffloading() {
    return backend_->IsEnableOffloading();
}

tl::expected<void, ErrorCode> KvclientStorageBackend::ScanMeta(
    const std::function<ErrorCode(
        const std::vector<std::string>& keys,
        std::vector<StorageObjectMetadata>& metadatas)>& handler) {
    if (!handler) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    // aistor 每次只扫描一页；Mooncake 仅调用一次 ScanMeta，因此这里必须
    // 持续扫描到空页。失败页不推进底层游标，调用方可重试该页。
    for (;;) {
        bool had_objects = false;
        auto result = backend_->ScanMeta(
            [&handler, &had_objects](
                const std::vector<std::string>& keys,
                std::vector<StorageObjectMetadata>& metadatas) {
                had_objects = !keys.empty();
                return handler(keys, metadatas);
            });
        if (!result || !had_objects) {
            return result;
        }
    }
}

void KvclientStorageBackend::ResetScanIterator() {
    backend_->ResetScanIterator();
}

void KvclientStorageBackend::RemoveAll() { backend_->RemoveAll(); }

tl::expected<std::vector<std::string>, ErrorCode>
KvclientStorageBackend::EvictAboveDiskWatermark(
    double high_watermark_ratio, double low_watermark_ratio,
    EvictionHandler eviction_handler) {
    return backend_->EvictAboveDiskWatermark(high_watermark_ratio,
                                             low_watermark_ratio,
                                             std::move(eviction_handler));
}

void KvclientStorageBackend::SetTestFailurePredicate(
    std::function<bool(const std::string& key)> predicate) {
    backend_->SetTestFailurePredicate(std::move(predicate));
}

}  // namespace mooncake

#endif  // MOONCAKE_WITH_KVCLIENT
