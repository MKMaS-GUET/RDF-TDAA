#ifndef CHARACTERISTIC_SET_HPP
#define CHARACTERISTIC_SET_HPP

#include <parallel_hashmap/btree.h>
#include <parallel_hashmap/phmap.h>
#include <span>
#include <vector>
#include "rdf-tdaa/utils/mmap.hpp"

class CharacteristicSet {
   public:
    struct Trie {
        phmap::btree_map<ulong, uint> next;
        uint cnt;
        uint set_cnt;
        phmap::flat_hash_map<uint, uint> exist;
        void DFS(uint node_id, std::vector<uint>& path, std::vector<std::vector<uint>>& result);

        Trie();
        ~Trie();

        uint Insert(std::vector<uint>& set);

        uint Find(std::vector<uint>& set);

        void Traverse(std::vector<std::vector<uint>>& result);
    };

   private:
    std::string file_path_;
    uint base_;
    MMap<uint8_t> mmap_;
    std::vector<std::pair<uint, uint>> offset_size_;
    std::vector<std::span<uint>> sets_;

   public:
    CharacteristicSet();
    CharacteristicSet(uint cnt);
    CharacteristicSet(std::string file_path);

    void Load();

    void Build(std::vector<std::pair<uint8_t*, uint>>& compressed_sets, std::vector<uint>& original_size);

    std::span<uint>& operator[](uint c_id);
};

#endif
