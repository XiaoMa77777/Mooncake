// Copyright (c) 2026 Hygon Information Technology Co., Ltd.
// SPDX-License-Identifier: Apache-2.0
//
// kvclient offload backend. Only compiled when WITH_KVCLIENT_BACKEND is ON; the
// StorageBackendType::kKvclient enumerator exists either way so that a
// misconfigured deployment fails with a clear message instead of silently
// falling back to another backend.

#pragma once

#include "storage_backend.h"

#if defined(MOONCAKE_WITH_KVCLIENT)

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

// 不把 SDK 头文件依赖传递给 Mooncake 的普通消费端。
namespace aistor {
class SdkTransport;
class KvclientStorageBackend;
}  // namespace aistor

namespace mooncake {

// 复用 aistor::KvclientStorageBackend 的存储实现，只负责配置、生命周期和
// 将分页 ScanMeta 转为 Mooncake 要求的单次完整扫描。
class KvclientStorageBackend final : public StorageBackendInterface {
   public:
    KvclientStorageBackend(const FileStorageConfig& config,
                           const KvclientBackendConfig& kvclient_config);
    ~KvclientStorageBackend() override;

    KvclientStorageBackend(const KvclientStorageBackend&) = delete;
    KvclientStorageBackend& operator=(const KvclientStorageBackend&) = delete;

    tl::expected<void, ErrorCode> Init() override;

    tl::expected<int64_t, ErrorCode> BatchOffload(
        const std::unordered_map<std::string, std::vector<Slice>>& batch_object,
        std::function<ErrorCode(const std::vector<std::string>& keys,
                                std::vector<StorageObjectMetadata>& metadatas)>
            complete_handler,
        EvictionHandler eviction_handler = nullptr) override;

    tl::expected<void, ErrorCode> BatchLoad(
        std::unordered_map<std::string, Slice>& batched_slices) override;

    tl::expected<bool, ErrorCode> IsExist(const std::string& key) override;

    tl::expected<bool, ErrorCode> IsEnableOffloading() override;

    tl::expected<void, ErrorCode> ScanMeta(
        const std::function<ErrorCode(
            const std::vector<std::string>& keys,
            std::vector<StorageObjectMetadata>& metadatas)>& handler) override;

    void ResetScanIterator() override;

    void RemoveAll() override;

    tl::expected<std::vector<std::string>, ErrorCode> EvictAboveDiskWatermark(
        double high_watermark_ratio, double low_watermark_ratio,
        EvictionHandler eviction_handler = nullptr) override;

    void SetTestFailurePredicate(
        std::function<bool(const std::string& key)> predicate) override;

   private:
    // 声明顺序保证 transport_ 的生命周期覆盖 backend_。
    std::shared_ptr<aistor::SdkTransport> transport_;
    std::unique_ptr<aistor::KvclientStorageBackend> backend_;
};

}  // namespace mooncake

#endif  // MOONCAKE_WITH_KVCLIENT
