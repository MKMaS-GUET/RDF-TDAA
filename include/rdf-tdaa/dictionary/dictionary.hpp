#ifndef DICTIONARY_HPP
#define DICTIONARY_HPP

#include <malloc.h>
#include <parallel_hashmap/phmap.h>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <variant>
#include <vector>
#include "rdf-tdaa/parser/sparql_parser.hpp"
#include "rdf-tdaa/utils/mmap.hpp"
#include "rdf-tdaa/utils/vbyte.hpp"

template <typename Key, typename Value>
using hash_map = phmap::flat_hash_map<Key, Value>;

enum Order { kSPO, kOPS };
enum Map { kSubjectMap, kPredicateMap, kObjectMap, kSharedMap };

class Dictionary {
    template <typename T>
    class Node {
        T* offsets_;
        ulong size_;
        MMap<char> node_file_;

       public:
        Node() : offsets_(0), size_(0) {}
        Node(std::string node_path) : offsets_(0), size_(0) {
            auto [data, size] = LoadAndDecompress(node_path + "/id2offset");
            size_ = size;
            if (sizeof(T) == 4) {
                offsets_ = reinterpret_cast<T*>(data);
            } else if (sizeof(T) == 8) {
                offsets_ = new T[size_ / 2];
                uint id = 0;
                for (ulong i = 0; i < size_; i++) {
                    if (i % 2 == 0) {
                        offsets_[id] = ulong(data[i]) << 32;
                    } else {
                        offsets_[id] |= data[i];
                        id++;
                    }
                }
                size_ /= 2;
                delete[] data;
            }
            node_file_ = MMap<char>(node_path + "/nodes");
        }

        char* operator[](uint id) {
            id -= 1;
            ulong start_offset = id ? offsets_[id - 1] : 0;
            ulong end_offset = offsets_[id] - 1;
            char* node = new char[end_offset - start_offset + 1];
            node[end_offset - start_offset] = '\0';
            for (uint i = 0; start_offset < end_offset; i++, start_offset++)
                node[i] = node_file_[start_offset];
            return node;
        }
    };

    std::string dict_path_;
    uint max_threads = 6;

    ulong subject_cnt_;
    ulong predicate_cnt_;
    ulong object_cnt_;
    ulong shared_cnt_;

    MMap<std::size_t> subject_hashes_;
    MMap<std::size_t> object_hashes_;
    MMap<std::size_t> shared_hashes_;
    MMap<uint> subject_ids_;
    MMap<uint> object_ids_;
    MMap<uint> shared_ids_;
    hash_map<std::string, uint> predicate2id_;

    std::vector<std::string> id2predicate_;
    std::variant<Node<uint>, Node<ulong>> id2subject_;
    std::variant<Node<uint>, Node<ulong>> id2object_;
    std::variant<Node<uint>, Node<ulong>> id2shared_;

    bool LoadPredicate(std::vector<std::string>& id2predicate, hash_map<std::string, uint>& predicate2id);

    long binarySearch(MMap<std::size_t> arr, long length, std::size_t target);

    uint Find(Map map, const std::string& str);

    uint FindInMaps(uint cnt, Map map, const std::string& str);

   public:
    Dictionary();

    Dictionary(std::string& dict_path_);

    void Close();

    const char* ID2String(uint id, SPARQLParser::Term::Positon pos);

    uint String2ID(const std::string& str, SPARQLParser::Term::Positon pos);

    uint subject_cnt();

    uint predicate_cnt();

    uint object_cnt();

    uint shared_cnt();

    uint max_id();
};

#endif