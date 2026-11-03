#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "storage_backend.h"
#include "types.h"

namespace mooncake {
class Client;
class FileStorage;

namespace detail {

// 旁路状态保持 Client/FileStorage 的大小及既有虚表不变，兼容现有 wheel。
// 读门覆盖源地址查询、复制及通知；写门覆盖 allocator 重挂，禁止检查后复用。
class OffloadEpoch {
   public:
    class ReadGuard {
       public:
        explicit ReadGuard(OffloadEpoch& epoch)
            : lock_(epoch.mutex_), generation_(epoch.epoch_.load()) {}
        uint64_t generation() const { return generation_; }
        bool Matches(uint64_t expected) const { return generation_ == expected; }

       private:
        std::shared_lock<std::shared_mutex> lock_;
        uint64_t generation_;
    };
    class WriteGuard {
       public:
        explicit WriteGuard(OffloadEpoch& epoch) : lock_(epoch.mutex_) {
            epoch.epoch_.fetch_add(1);
        }

       private:
        std::unique_lock<std::shared_mutex> lock_;
    };
    ReadGuard Read() { return ReadGuard(*this); }
    WriteGuard Advance() { return WriteGuard(*this); }
    uint64_t Current() const { return epoch_.load(); }

   private:
    std::shared_mutex mutex_;
    std::atomic<uint64_t> epoch_{1};
};

std::shared_ptr<OffloadEpoch> GetClientOffloadEpoch(const Client* client);

struct FileStorageRecoveryState {
    std::shared_ptr<OffloadEpoch> epoch;
    uint64_t observed_epoch{0};
    // 只有心跳线程读取源地址；测试可通过 query 注入确定性输入。
    std::chrono::steady_clock::time_point source_lease_deadline{
        std::chrono::steady_clock::time_point::max()};
    std::function<tl::expected<void, ErrorCode>(
        const std::vector<std::string>&, const std::string&,
        std::unordered_map<std::string, std::vector<Slice>>&)> query;
    std::function<tl::expected<void, ErrorCode>(
        bool, std::vector<OffloadTaskItem>&)> fetch;
    std::function<tl::expected<void, ErrorCode>(bool)> mount;
    std::function<tl::expected<void, ErrorCode>(int64_t)> report;
    std::function<tl::expected<void, ErrorCode>(
        const std::vector<OffloadTaskItem>&,
        const std::vector<StorageObjectMetadata>&)> notify;
    std::function<tl::expected<bool, ErrorCode>()> poll_remove;

    uint64_t RequestScan() { return requested_.fetch_add(1) + 1; }
    uint64_t RequestedScan() const { return requested_.load(); }
    bool HasPendingScan() const { return completed_.load() < requested_.load(); }
    void CompleteScan(uint64_t ticket) {
        auto previous = completed_.load();
        while (previous < ticket &&
               !completed_.compare_exchange_weak(previous, ticket)) {
        }
    }
    // 恢复边界各自独立上报（计划 §4.6）：已持久提交由后端索引负责，这里只报告
    // 本进程观测到的登记、待恢复、取消与耗时，避免「整批完成」这种模糊口径。
    uint64_t CompletedScan() const { return completed_.load(); }
    uint64_t Recoveries() const { return recoveries_.load(); }
    uint64_t RegisteredObjects() const { return registered_objects_.load(); }
    uint64_t LastRecoveryMs() const { return last_recovery_ms_.load(); }
    uint64_t SlowTicks() const { return slow_ticks_.load(); }
    uint64_t CancelledTasks() const { return cancelled_tasks_.load(); }
    uint64_t FollowupScans() const { return followup_scans_.load(); }
    // Why a fetched task was reported back to the master as failed. The two
    // causes have different owners: a missing source means the memory replica
    // was already gone, everything else is a local staging or backend failure.
    void RecordNack(bool source_missing) {
        if (source_missing) nack_source_missing_.fetch_add(1);
        else nack_other_.fetch_add(1);
    }
    uint64_t NackSourceMissing() const { return nack_source_missing_.load(); }
    uint64_t NackOther() const { return nack_other_.load(); }
    void RecordLeaseDiscard(uint64_t objects) { lease_discarded_.fetch_add(objects); }
    // One counter per failed-task site: the aggregate log alone cannot say
    // which branch reported a task, and the branches have different owners.
    enum class NackSite { kSourceMissing, kD2H, kLeaseDiscard, kBucketFailed, kUnvisited };
    void RecordNackSite(NackSite site, uint64_t count = 1) {
        nack_site_[static_cast<size_t>(site)].fetch_add(count);
    }
    uint64_t NackSite(NackSite site) const {
        return nack_site_[static_cast<size_t>(site)].load();
    }
    uint64_t LeaseDiscarded() const { return lease_discarded_.load(); }

    // Long enough to outlive the Master's client TTL (DEFAULT_CLIENT_LIVE_TTL_SEC
    // = 10s) so the dead owner's replica is gone before the follow-up runs.
    static constexpr uint64_t kFollowupScanDelayMs = 30000;

    void ScheduleFollowupScan(uint64_t delay_ms) {
        const auto now = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now().time_since_epoch())
                .count());
        followup_scan_due_ms_.store(now + delay_ms, std::memory_order_release);
    }
    // True at most once per scheduled follow-up, and only after it is due.
    bool ConsumeFollowupScan() {
        const auto due = followup_scan_due_ms_.load(std::memory_order_acquire);
        if (due == 0) return false;
        const auto now = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now().time_since_epoch())
                .count());
        if (now < due) return false;
        followup_scan_due_ms_.store(0, std::memory_order_release);
        followup_scans_.fetch_add(1);
        return true;
    }
    void RecordRecovery(uint64_t registered, uint64_t elapsed_ms) {
        if (registered) {
            registered_objects_.fetch_add(registered);
            // A restarted client's re-registration does NOT take over a
            // LOCAL_DISK replica still owned by the previous process:
            // MasterService::NotifyOffloadSuccess only refreshes replicas whose
            // client_id matches the caller, and skips adding one while any
            // local-disk replica exists. That stale owner is only dropped when
            // the Master reaps the dead client on its client TTL, so this
            // client can claim the object only on a scan that runs after that
            // window. Schedule exactly one bounded follow-up scan.
            ScheduleFollowupScan(kFollowupScanDelayMs);
        }
        recoveries_.fetch_add(1);
        last_recovery_ms_.store(elapsed_ms);
    }
    void RecordSlowTick() { slow_ticks_.fetch_add(1); }
    void RecordCancelledTasks(uint64_t count) {
        cancelled_tasks_.fetch_add(count);
    }

   private:
    std::atomic<uint64_t> requested_{0};
    std::atomic<uint64_t> completed_{0};
    std::atomic<uint64_t> recoveries_{0};
    std::atomic<uint64_t> registered_objects_{0};
    std::atomic<uint64_t> last_recovery_ms_{0};
    std::atomic<uint64_t> slow_ticks_{0};
    std::atomic<uint64_t> cancelled_tasks_{0};
    std::atomic<uint64_t> followup_scans_{0};
    std::atomic<uint64_t> nack_source_missing_{0};
    std::atomic<uint64_t> nack_other_{0};
    std::atomic<uint64_t> lease_discarded_{0};
    std::array<std::atomic<uint64_t>, 5> nack_site_{};
    std::atomic<uint64_t> followup_scan_due_ms_{0};
};

// 一次性快照，供日志和测试读取，不做跨字段的强一致承诺。
struct OffloadRecoverySnapshot {
    uint64_t generation{0};
    uint64_t scans_requested{0};
    uint64_t scans_completed{0};
    uint64_t pending_scan{0};
    uint64_t recoveries{0};
    uint64_t registered_objects{0};
    uint64_t last_recovery_ms{0};
    uint64_t slow_ticks{0};
    uint64_t cancelled_tasks{0};
};

inline OffloadRecoverySnapshot TakeSnapshot(
    const FileStorageRecoveryState& state,
    const OffloadEpoch& epoch) {
    OffloadRecoverySnapshot snapshot;
    snapshot.generation = epoch.Current();
    snapshot.scans_requested = state.RequestedScan();
    snapshot.scans_completed = state.CompletedScan();
    snapshot.pending_scan = state.HasPendingScan() ? 1 : 0;
    snapshot.recoveries = state.Recoveries();
    snapshot.registered_objects = state.RegisteredObjects();
    snapshot.last_recovery_ms = state.LastRecoveryMs();
    snapshot.slow_ticks = state.SlowTicks();
    snapshot.cancelled_tasks = state.CancelledTasks();
    return snapshot;
}

std::shared_ptr<FileStorageRecoveryState> GetFileStorageRecoveryState(
    const FileStorage* storage);

}  // namespace detail
}  // namespace mooncake
