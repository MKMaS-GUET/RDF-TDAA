#ifndef PREDICATE_SET_TRIE_HPP
#define PREDICATE_SET_TRIE_HPP

#include <parallel_hashmap/btree.h>
#include <parallel_hashmap/phmap.h>
#include <sys/types.h>
#include <stack>
#include <vector>

class PredicateSetTrie {
    phmap::btree_map<ulong, uint> next_;
    uint cnt_;
    uint set_cnt_;
    phmap::flat_hash_map<uint, uint> exist_;
    void dfs(uint node_id, std::vector<uint>& path, std::vector<std::vector<uint>>& result);

   public:
    PredicateSetTrie();
    ~PredicateSetTrie();

    uint insert(std::vector<uint>& set);

    uint find(std::vector<uint>& set);

    void traverse(std::vector<std::vector<uint>>& result);
};

#endif