#include <malloc.h>
#include <parallel_hashmap/btree.h>
#include <parallel_hashmap/phmap.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <thread>

#include "rdf-tdaa/utils/mmap.hpp"

template <typename Key, typename Value>
using hash_map = phmap::flat_hash_map<Key, Value>;

class DictionaryBuilder {
    std::string dict_path_;
    std::string file_path_;
    uint triplet_cnt_ = 0;
    MMap<uint> menagement_data_;

    hash_map<std::string, uint> subjects_;
    hash_map<std::string, uint> predicates_;
    hash_map<std::string, uint> objects_;
    hash_map<std::string, uint> shared_;

    void Init();

    void BuildDict();

    void ReassignIDAndSave(hash_map<std::string, uint>& map,
                           std::ofstream& dict_out,
                           std::string hashmap_path,
                           uint menagement_file_offset,
                           uint max_threads);

    void SaveDict(uint max_threads) ;

   public:
    DictionaryBuilder(std::string& dict_path, std::string& file_path);

    void Build();

    void EncodeRDF(hash_map<uint, std::vector<std::pair<uint, uint>>>& pso);

    void Close();
};
