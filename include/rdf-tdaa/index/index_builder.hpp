#ifndef INDEX_BUILDER_HPP
#define INDEX_BUILDER_HPP

#include <parallel_hashmap/btree.h>
#include <parallel_hashmap/phmap.h>
#include <filesystem>
#include <future>
#include <iostream>
#include <mutex>
#include <queue>
#include <string>
#include <vector>

#include "rdf-tdaa/dictionary/dictionary.hpp"
#include "rdf-tdaa/dictionary/dictionary_builder.hpp"
#include "rdf-tdaa/utils/bitset.hpp"
#include "rdf-tdaa/utils/daa.hpp"
#include "rdf-tdaa/utils/mmap.hpp"
#include "rdf-tdaa/utils/predicate_set_trie.hpp"
#include "streamvbyte.h"

namespace fs = std::filesystem;

class IndexBuilder {
    struct PredicateIndex {
        phmap::btree_set<uint> s_set;
        phmap::btree_set<uint> o_set;

        void Build(std::vector<std::pair<uint, uint>>& so_pairs);

        void Clear();
    };

    std::string data_file_;
    std::string db_index_path_;
    std::string db_dictionary_path_;
    std::string db_name_;
    ulong all_arr_size_ = 0;

    bool compress_predicate_index_ = true;
    bool compress_to_daa_ = true;
    bool compress_levels_ = true;

    Dictionary dict_;

    std::shared_ptr<hash_map<uint, std::vector<std::pair<uint, uint>>>> pso_;

    std::mutex mtx_;

    void BuildPredicateIndex(std::vector<PredicateIndex>& predicate_indexes);

    // void SubBuildPredicateIndex(std::queue<uint>* task_queue,
    //                             std::vector<PredicateIndex>* predicate_indexes,
    //                             double* finished);
    void SubBuildPredicateIndex(std::deque<uint>* task_queue,
                                std::mutex* task_queue_mutex,
                                std::condition_variable* task_queue_cv,
                                std::atomic<bool>* task_queue_empty,
                                std::vector<PredicateIndex>* local_predicate_indexes);

    void StorePredicateIndexNoCompress(std::vector<PredicateIndex>& predicate_indexes);

    void StorePredicateIndex(std::vector<PredicateIndex>& predicate_indexes);

    void BuildCharacteristicSet(std::vector<std::pair<uint, uint>>& to_set_id, Order order);

    uint BuildEntitySets(std::vector<std::pair<uint, uint>>& to_set_id,
                         std::vector<std::vector<std::vector<uint>>>& entity_set,
                         Order order);

    void BuildAndSaveIndex(std::vector<std::pair<uint, uint>>& to_set_id,
                           std::vector<std::vector<std::vector<uint>>>& entity_set,
                           uint levels_width,
                           Order order);

   public:
    IndexBuilder(std::string db_name, std::string data_file);

    bool Build();
};

#endif