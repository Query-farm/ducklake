//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/ducklake_lru.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/unordered_map.hpp"

#include <functional>
#include <list>

namespace duckdb {

//! Thread-safe LRU cache supporting either count-bounded or byte-bounded
//! eviction. Pull-through: callers use GetOrLoad(key, loader). Loader
//! exceptions do not poison the cache. No single-flight on miss — concurrent
//! misses run the loader independently and last-writer-wins on Put.
//!
//! Budget mode is selected at construction:
//!   - DuckLakeLRU<K,V>::ByCount(max_entries)
//!   - DuckLakeLRU<K,V>::ByBytes(max_bytes, sizer)
//! For ByBytes, callers either pass a per-value size to GetOrLoad/Put, or
//! provide a sizer functor at construction time that derives a size from V.
template <class K, class V, class Hash = std::hash<K>, class Eq = std::equal_to<K>>
class DuckLakeLRU {
public:
	using Sizer = std::function<idx_t(const V &)>;

	enum class BudgetKind { COUNT, BYTES };

	enum class Event { HIT, MISS, EVICTION };

	//! Event callback signature. Called for hit, miss, and each eviction.
	//! NOTE: invoked while the cache mutex is held — keep callbacks fast and
	//! avoid recursive cache access. The LRU collects evicted keys before
	//! firing the callback to minimize lock-hold time for the eviction case.
	using EventCallback = std::function<void(Event, const K &)>;

	struct Stats {
		uint64_t hits = 0;
		uint64_t misses = 0;
		uint64_t evictions = 0;
		idx_t entries = 0;
		idx_t total_bytes = 0;
	};

	struct ByCountTag {};
	struct ByBytesTag {};

private:
	struct Entry {
		K key;
		V value;
		idx_t size_bytes; // 1 for COUNT-mode entries
	};
	using ListType = std::list<Entry>;
	using ListIter = typename ListType::iterator;

public:
	//! Construct a count-bounded LRU. Each entry counts as 1.
	DuckLakeLRU(ByCountTag, idx_t max_entries) : budget_kind(BudgetKind::COUNT), budget(max_entries) {
	}
	//! Construct a byte-bounded LRU. Each entry's size is computed by sizer.
	DuckLakeLRU(ByBytesTag, idx_t max_bytes, Sizer sizer_p)
	    : budget_kind(BudgetKind::BYTES), budget(max_bytes), sizer(std::move(sizer_p)) {
	}

	//! Set an optional event callback. Replaces any previously-set callback.
	//! Setting before the cache sees traffic avoids a race; setting later is
	//! safe but events between the unlocked window may be missed.
	void SetEventCallback(EventCallback cb) {
		lock_guard<mutex> guard(lock);
		event_callback = std::move(cb);
	}

	DuckLakeLRU(const DuckLakeLRU &) = delete;
	DuckLakeLRU &operator=(const DuckLakeLRU &) = delete;
	DuckLakeLRU(DuckLakeLRU &&) = delete;
	DuckLakeLRU &operator=(DuckLakeLRU &&) = delete;

	//! Pull-through lookup. On miss the loader is invoked outside the cache
	//! lock to avoid serializing unrelated keys. Loader exceptions are
	//! propagated and leave the cache unchanged. If the loaded value is
	//! larger than the entire budget, it is returned to the caller but not
	//! retained in the cache.
	template <class Loader>
	V GetOrLoad(const K &key, Loader loader) {
		{
			lock_guard<mutex> guard(lock);
			auto it = index.find(key);
			if (it != index.end()) {
				stats.hits++;
				list.splice(list.begin(), list, it->second);
				if (event_callback) {
					event_callback(Event::HIT, key);
				}
				return it->second->value;
			}
			stats.misses++;
			if (event_callback) {
				event_callback(Event::MISS, key);
			}
		}
		// loader runs outside the lock
		V value = loader();
		Put(key, value, ComputeSize(value));
		return value;
	}

	//! Insert or replace. Size is taken from the sizer (for ByBytes) or 1 (for
	//! ByCount). Triggers eviction if the budget is exceeded.
	void Put(const K &key, V value, idx_t size_hint = 0) {
		idx_t size = (budget_kind == BudgetKind::BYTES) ? (size_hint == 0 ? sizer(value) : size_hint) : 1;
		lock_guard<mutex> guard(lock);
		auto it = index.find(key);
		if (it != index.end()) {
			total_size -= it->second->size_bytes;
			it->second->value = std::move(value);
			it->second->size_bytes = size;
			total_size += size;
			list.splice(list.begin(), list, it->second);
		} else {
			list.push_front(Entry {key, std::move(value), size});
			index[key] = list.begin();
			total_size += size;
		}
		EvictWhileOverBudget();
	}

	//! Erase entries matching the predicate. Predicate is called with const K&.
	template <class Pred>
	void EraseIf(Pred pred) {
		lock_guard<mutex> guard(lock);
		for (auto it = list.begin(); it != list.end();) {
			if (pred(it->key)) {
				total_size -= it->size_bytes;
				if (event_callback) {
					event_callback(Event::EVICTION, it->key);
				}
				index.erase(it->key);
				it = list.erase(it);
				stats.evictions++;
			} else {
				++it;
			}
		}
	}

	void Clear() {
		lock_guard<mutex> guard(lock);
		index.clear();
		list.clear();
		total_size = 0;
	}

	Stats GetStats() const {
		lock_guard<mutex> guard(lock);
		Stats snapshot = stats;
		snapshot.entries = index.size();
		snapshot.total_bytes = total_size;
		return snapshot;
	}

private:
	idx_t ComputeSize(const V &value) const {
		if (budget_kind == BudgetKind::COUNT) {
			return 1;
		}
		return sizer ? sizer(value) : 0;
	}

	void EvictWhileOverBudget() {
		while (!list.empty() && total_size > budget) {
			auto &back = list.back();
			total_size -= back.size_bytes;
			if (event_callback) {
				event_callback(Event::EVICTION, back.key);
			}
			index.erase(back.key);
			list.pop_back();
			stats.evictions++;
		}
	}

	mutable mutex lock;
	BudgetKind budget_kind;
	idx_t budget;
	Sizer sizer;

	ListType list; // front = most recently used
	unordered_map<K, ListIter, Hash, Eq> index;
	idx_t total_size = 0;
	Stats stats;
	EventCallback event_callback;
};

} // namespace duckdb
