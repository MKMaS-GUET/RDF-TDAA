#include "rdf-tdaa/utils/predicate_set_trie.hpp"

PredicateSetTrie::PredicateSetTrie() {
    cnt_ = 0;
    set_cnt_ = 1;
}

PredicateSetTrie::~PredicateSetTrie() {
    phmap::btree_map<ulong, uint>().swap(next_);
    phmap::flat_hash_map<uint, uint>().swap(exist_);
}

uint PredicateSetTrie::insert(std::vector<uint>& set) {
    uint node_id = 0;
    uint next_id = 0;

    for (auto it = set.begin(); it != set.end(); it++) {
        uint code = *it - 1;
        next_id = next_[((ulong)node_id << 32) | code];
        if (!next_id) {
            next_[((ulong)node_id << 32) | code] = ++cnt_;
            next_id = cnt_;
        }
        node_id = next_id;
    }
    auto it = exist_.insert({node_id, set_cnt_});
    if (it.second)
        set_cnt_++;
    return it.first->second;
}

uint PredicateSetTrie::find(std::vector<uint>& set) {
    uint node_id = 0;
    uint next_id = 0;

    for (auto it = set.begin(); it != set.end(); it++) {
        next_id = next_[((ulong)node_id << 32) | (*it - 1)];
        node_id = next_id;
    }
    return exist_[node_id];
}

void PredicateSetTrie::traverse(std::vector<std::vector<uint>>& result) {
    std::vector<uint> path;
    dfs(0, path, result);
}

void PredicateSetTrie::dfs(uint node_id, std::vector<uint>& path, std::vector<std::vector<uint>>& result) {
    if (exist_[node_id] > 0) {
        result.push_back(path);
    }

    auto beg = next_.lower_bound((ulong)node_id << 32);
    auto end = next_.lower_bound((ulong)(node_id + 1) << 32);

    uint next_id;
    for (auto it = beg; it != end; it++) {
        next_id = it->second;
        if (next_id) {
            path.push_back((uint)(it->first & 0xFFFFFFFF) + 1);
            dfs(next_id, path, result);
            path.pop_back();
        }
    }
};
