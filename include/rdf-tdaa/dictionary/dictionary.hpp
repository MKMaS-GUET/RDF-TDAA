#ifndef DICTIONARY_HPP
#define DICTIONARY_HPP

#include <malloc.h>
#include <parallel_hashmap/phmap.h>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <vector>

#include "rdf-tdaa/utils/mmap.hpp"

template <typename Key, typename Value>
using hash_map = phmap::flat_hash_map<Key, Value>;

enum Order { kSPO, kOPS };
enum Pos { kSubject, kPredicate, kObject, kShared };
enum Map { kSubjectMap, kPredicateMap, kObjectMap, kSharedMap };

class Dictionary {
    class Loader {
        std::string dict_path_;
        uint subject_cnt_;
        uint object_cnt_;
        uint shared_cnt_;

        void SubLoadID2Node(std::vector<std::string>& id2node,
                            MMap<char>& node_file,
                            uint start_id,
                            std::size_t start_offset,
                            std::size_t end_offset);

       public:
        Loader(std::string dict_path, uint subject_cnt, uint object_cnt, uint shared_cnt);

        bool LoadPredicate(std::vector<std::string>& id2predicate, hash_map<std::string, uint>& predicate2id);

        void LoadID2Node(std::vector<std::string>& id2subject_,
                         std::vector<std::string>& id2object_,
                         std::vector<std::string>& id2shared_);
    };

    std::string dict_path_;
    uint max_threads = 6;

    uint subject_cnt_;
    uint predicate_cnt_;
    uint object_cnt_;
    uint shared_cnt_;
    uint triplet_cnt_;

    MMap<std::size_t> subject_hashes_;
    MMap<std::size_t> object_hashes_;
    MMap<std::size_t> shared_hashes_;
    MMap<uint> subject_ids_;
    MMap<uint> object_ids_;
    MMap<uint> shared_ids_;
    hash_map<std::string, uint> predicate2id_;

    std::vector<std::string> id2subject_;
    std::vector<std::string> id2predicate_;
    std::vector<std::string> id2object_;
    std::vector<std::string> id2shared_;

    void InitLoad();

    long binarySearch(MMap<std::size_t> arr, long length, std::size_t target);

    uint Find(Map map, const std::string& str);

    uint FindInMaps(uint cnt, Map map, const std::string& str);

   public:
    Dictionary();

    Dictionary(std::string& dict_path_);

    void Close();

    void Load();

    std::string& ID2String(uint id, Pos pos);

    uint String2ID(const std::string& str, Pos pos);

    uint subject_cnt();

    uint predicate_cnt();

    uint object_cnt();

    uint shared_cnt();

    uint triplet_cnt();

    uint max_id();
};

#endif