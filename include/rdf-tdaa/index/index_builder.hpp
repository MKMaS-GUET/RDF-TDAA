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
#include "rdf-tdaa/index/daas.hpp"
#include "rdf-tdaa/index/predicate_index.hpp"

namespace fs = std::filesystem;

class IndexBuilder {
    enum class Permutation { kSPO, kOPS };

    std::string data_file_;
    std::string db_index_path_;
    std::string db_dictionary_path_;
    std::string spo_index_path_;
    std::string ops_index_path_;
    std::string db_name_;
    ulong all_arr_size_ = 0;

    bool compress_predicate_index_ = true;
    bool compress_to_daa_ = true;
    bool compress_levels_ = true;

    Dictionary dict_;

    std::shared_ptr<hash_map<uint, std::vector<std::pair<uint, uint>>>> pso_;

    std::mutex mtx_;

    void BuildCharacteristicSet(std::vector<uint>& c_set_id,
                                std::vector<uint>& c_set_size,
                                Permutation permutation);

    void BuildEntitySets(PredicateIndex& predicate_index,
                         std::vector<uint>& c_set_size,
                         std::vector<std::vector<std::vector<uint>>>& entity_set,
                         Permutation permutation);

   public:
    IndexBuilder(std::string db_name, std::string data_file);

    bool Build();
};

#endif