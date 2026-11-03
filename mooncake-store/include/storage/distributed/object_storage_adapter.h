#pragma once

#include <sys/uio.h>

#include <cstring>
#include <limits>
#include <new>
#include <span>
#include <string>
#include <vector>
#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

struct KeyInfo {
    // Decoded Mooncake logical key, not a provider-specific physical key.
    std::string logical_key;
    size_t size = 0;
};

// One object to write. `logical_key` is the Mooncake logical key and may contain
// arbitrary bytes; `iovs` concatenate, in order, to the object's bytes.
struct ObjectPutRequest {
    std::string logical_key;
    std::vector<iovec> iovs;
};

// One object to read. `iovs` concatenate, in order, to cover the *whole* object,
// so their total length must equal `expected_length`. Partial and ranged reads
// are not part of this contract: an implementation that cannot cover the whole
// object must refuse the item rather than satisfy each region with its own read
// from the object's start.
struct ObjectGetRequest {
    std::string logical_key;
    std::vector<iovec> iovs;
    size_t expected_length = 0;
};

/**
 * @brief Adapts object storage services to the distributed backend's
 * key-value I/O contract.
 *
 * All key parameters are opaque Mooncake logical keys and may contain
 * arbitrary bytes. Implementations own the reversible mapping between logical
 * keys and their configured physical object namespace, including prefixing,
 * encoding, and provider-specific validation.
 */
class ObjectStorageAdapter {
   public:
    virtual ~ObjectStorageAdapter() = default;

    virtual tl::expected<void, ErrorCode> Put(const std::string& logical_key,
                                              std::span<const char> data) = 0;

    // Atomic multi-region write: one commit and one result.
    virtual tl::expected<void, ErrorCode> PutV(const std::string& logical_key,
                                               const iovec* iov,
                                               int iovcnt) = 0;

    // Reads the whole object and reports its real length.
    //
    // An object that does not fit in `len` must be REFUSED, not truncated: a
    // truncated read reports a length that looks like success while having
    // delivered part of an object, and every caller above this layer would then
    // have to re-derive the real size to notice. Returning `min(len, actual)` is
    // therefore a contract violation, not a lenient success.
    virtual tl::expected<size_t, ErrorCode> Get(const std::string& logical_key,
                                                void* buf, size_t len) = 0;

    virtual tl::expected<bool, ErrorCode> Exists(
        const std::string& logical_key) = 0;

    virtual tl::expected<void, ErrorCode> Delete(
        const std::string& logical_key) = 0;

    // Pagination is an implementation detail. Returns decoded logical keys
    // from the adapter's configured physical namespace.
    virtual tl::expected<std::vector<KeyInfo>, ErrorCode> ListKeys() = 0;

    virtual tl::expected<void, ErrorCode> Init() = 0;
    virtual const char* GetName() const = 0;

    // Capability bit: true only when this adapter can carry *device* Value bytes
    // through its object entry points without an HOST staging copy -- that is,
    // when it has a registered application device arena behind it and validates
    // every submitted range itself.
    //
    // The caller (the DFS write path) uses this to decide whether it may skip its
    // own device-to-HOST staging. It is a *capability*, not a permission: an
    // adapter that reports true still has to refuse an individual request whose
    // range is not a fully registered device allocation, and it must refuse
    // rather than fall back to a HOST copy. Default false keeps every existing
    // adapter, and every HOST-only deployment, byte-for-byte unchanged.
    virtual bool AcceptsDevicePointers() const { return false; }

    // Is this exact range one the adapter can carry as device Value? True only
    // when the range lies fully inside an arena the adapter actually registered.
    // The default (no device support) is false. The caller uses it to decide
    // between two distinguishable answers for device memory it found through its
    // own accelerator registry: "the adapter owns this range" versus "this is
    // device memory the adapter does not know" -- the latter must be refused
    // rather than handed over as if it were a HOST buffer.
    virtual bool AcceptsDeviceRange(const void* address, std::size_t length) const {
        (void)address;
        (void)length;
        return false;
    }

    // Shutdown. Stops admitting work, drains what is in flight, proves no DMA can
    // still touch a caller buffer, unregisters shared resources and only then
    // reports success. An implementation that cannot prove that yet must report
    // UNAVAILABLE_IN_CURRENT_STATUS and keep every resource alive; the caller
    // retries until OK before releasing device memory. The default is a no-op so
    // an adapter with nothing to drain compiles unchanged.
    virtual tl::expected<void, ErrorCode> Close() { return {}; }

    // Batch entry points. One item is one object. Results are returned in the
    // caller's order and are always `requests.size()` long -- an empty request
    // list yields an empty result list, never a short one. Each item is decided
    // independently: one item's failure never suppresses another's, and a caller
    // reading `results[i]` is reading `requests[i]`. An item that fails validation
    // never reaches the single-object entry points, and the items after it still
    // run.
    //
    // The default bodies below loop the single-object entry points. They exist so
    // an existing adapter compiles unchanged, and they are explicitly *serial*:
    // an implementation that can submit a whole batch across keys must override
    // them and do so, because a per-item loop here is the exact regression this
    // interface was added to remove.
    //
    // Caller buffers -- including the iovec arrays themselves -- and any metadata
    // describing them must stay valid until the whole call returns.
    //
    // Object bytes: an empty object (zero total bytes) is refused as
    // INVALID_PARAMS rather than being treated as a zero-length success, because
    // "nothing was stored" and "an empty value was stored" are different answers
    // and the contract has no way to tell them apart afterwards.
    virtual std::vector<tl::expected<void, ErrorCode>> PutVBatch(
        const std::vector<ObjectPutRequest>& requests) {
        std::vector<tl::expected<void, ErrorCode>> results;
        results.reserve(requests.size());
        for (const auto& request : requests) {
            size_t total = 0;
            if (!SumIovs(request.iovs, total) || total == 0 ||
                request.iovs.size() >
                    static_cast<size_t>(std::numeric_limits<int>::max())) {
                results.push_back(tl::make_unexpected(ErrorCode::INVALID_PARAMS));
                continue;
            }
            results.push_back(PutV(request.logical_key, request.iovs.data(),
                                   static_cast<int>(request.iovs.size())));
        }
        return results;
    }

    virtual std::vector<tl::expected<size_t, ErrorCode>> GetBatch(
        const std::vector<ObjectGetRequest>& requests) {
        std::vector<tl::expected<size_t, ErrorCode>> results;
        results.reserve(requests.size());
        for (const auto& request : requests) {
            size_t capacity = 0;
            // The regions must describe the whole object, exactly. Anything else
            // is refused here rather than silently turning into a ranged read per
            // region.
            if (!SumIovs(request.iovs, capacity) || capacity == 0 ||
                capacity != request.expected_length) {
                results.push_back(tl::make_unexpected(ErrorCode::INVALID_PARAMS));
                continue;
            }
            // A serial adapter has one destination buffer per call, so this
            // default gathers into a temporary and scatters. The staging budget is
            // explicit and per item: past it the item is refused rather than
            // allowed to consume the process's memory, and one item's allocation
            // failure becomes that item's error instead of ending the batch.
            if (capacity > kDefaultBatchScratchBudgetBytes) {
                results.push_back(tl::make_unexpected(ErrorCode::INVALID_PARAMS));
                continue;
            }
            std::vector<char> staged;
            try {
                staged.resize(capacity);
            } catch (const std::bad_alloc&) {
                results.push_back(
                    tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS));
                continue;
            }
            auto read = Get(request.logical_key, staged.data(), staged.size());
            if (!read) {
                results.push_back(tl::make_unexpected(read.error()));
                continue;
            }
            // `Get` reports the object's real length. It must not silently
            // truncate: a longer object returned as a short read would look like a
            // success here while having delivered half an object.
            if (*read != capacity) {
                results.push_back(tl::make_unexpected(ErrorCode::INVALID_PARAMS));
                continue;
            }
            size_t offset = 0;
            for (const auto& iov : request.iovs) {
                if (iov.iov_len != 0) {
                    std::memcpy(iov.iov_base, staged.data() + offset, iov.iov_len);
                }
                offset += iov.iov_len;
            }
            results.push_back(capacity);
        }
        return results;
    }

    // Staging budget for the default GetBatch, per item. A provider override
    // reads straight into the caller's regions and is not bound by it -- this
    // exists so the *compatibility* path cannot be talked into allocating an
    // arbitrarily large temporary. Public because a caller has to be able to
    // know the limit rather than discover it as a per-item refusal.
    static constexpr size_t kDefaultBatchScratchBudgetBytes =
        size_t{256} * 1024 * 1024;

   protected:
    // Sums the iovecs, refusing overflow and refusing a non-zero length with a
    // null base. Both are reachable from ordinary caller mistakes and both would
    // otherwise turn into a huge memcpy: SIZE_MAX+2 wraps to 1, which a matching
    // expected_length would accept.
    static bool SumIovs(const std::vector<iovec>& iovs, size_t& total) {
        total = 0;
        for (const auto& iov : iovs) {
            if (iov.iov_len != 0 && iov.iov_base == nullptr) {
                return false;
            }
            if (iov.iov_len > std::numeric_limits<size_t>::max() - total) {
                return false;
            }
            total += iov.iov_len;
        }
        return true;
    }
};

}  // namespace mooncake
