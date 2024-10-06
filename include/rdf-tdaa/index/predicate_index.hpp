#ifndef PREDICATE_INDEX_HPP
#define PREDICATE_INDEX_HPP

#include <parallel_hashmap/btree.h>
#include <parallel_hashmap/phmap.h>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <span>
#include <string>
#include <vector>
#include "rdf-tdaa/utils/mmap.hpp"
#include "sys/types.h"

class PredicateIndex {
   public:
    struct Index {
        std::vector<uint> s_set;
        std::vector<uint> o_set;

        phmap::flat_hash_map<uint, uint> sid2offset;
        phmap::flat_hash_map<uint, uint> oid2offset;

        void Build(std::vector<std::pair<uint, uint>>& so_pairs);

        void BuildMap();

        void Clear();
    };

    enum Type { kPO, kPS };

    std::vector<Index> index_;

   private:
    bool compress_predicate_index_ = true;
    std::string file_path_;
    std::shared_ptr<phmap::flat_hash_map<uint, std::vector<std::pair<uint, uint>>>> pso_;

    uint max_predicate_id_;
    MMap<uint> predicate_index_mmap_;
    MMap<uint> predicate_index_arrays_no_compress_;
    MMap<uint8_t> predicate_index_arrays_;
    std::vector<std::span<uint>> ps_sets_;
    std::vector<std::span<uint>> po_sets_;

    void BuildPredicateIndex();

    void SubBuildPredicateIndex(std::deque<uint>* task_queue,
                                std::mutex* task_queue_mutex,
                                std::condition_variable* task_queue_cv,
                                std::atomic<bool>* task_queue_empty);

    void StorePredicateIndexNoCompress();

    void StorePredicateIndex();

   public:
    PredicateIndex();
    PredicateIndex(std::string file_path, uint max_predicate_id);
    PredicateIndex(std::shared_ptr<phmap::flat_hash_map<uint, std::vector<std::pair<uint, uint>>>> pso,
                   std::string file_path,
                   uint max_predicate_id);

    void Build();

    void Store();

    std::span<uint>& GetSSet(uint pid);

    std::span<uint>& GetOSet(uint pid);

    uint GetSSetSize(uint pid);

    uint GetOSetSize(uint pid);

    void Close();
};

#endif