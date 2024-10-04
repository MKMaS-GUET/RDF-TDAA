#include "rdf-tdaa/index/characteristic_set.hpp"
#include <iostream>
#include "rdf-tdaa/utils/vbyte.hpp"

CharacteristicSet::Trie::Trie() {
    cnt = 0;
    set_cnt = 1;
}

CharacteristicSet::Trie::~Trie() {
    phmap::btree_map<ulong, uint>().swap(next);
    phmap::flat_hash_map<uint, uint>().swap(exist);
}

uint CharacteristicSet::Trie::Insert(std::vector<uint>& set) {
    uint node_id = 0;
    uint next_id = 0;

    for (auto it = set.begin(); it != set.end(); it++) {
        uint code = *it - 1;
        next_id = next[((ulong)node_id << 32) | code];
        if (!next_id) {
            next[((ulong)node_id << 32) | code] = ++cnt;
            next_id = cnt;
        }
        node_id = next_id;
    }
    auto it = exist.insert({node_id, set_cnt});
    if (it.second)
        set_cnt++;
    return it.first->second;
}

uint CharacteristicSet::Trie::Find(std::vector<uint>& set) {
    uint node_id = 0;
    uint next_id = 0;

    for (auto it = set.begin(); it != set.end(); it++) {
        next_id = next[((ulong)node_id << 32) | (*it - 1)];
        node_id = next_id;
    }
    return exist[node_id];
}

void CharacteristicSet::Trie::Traverse(std::vector<std::vector<uint>>& result) {
    std::vector<uint> path;
    DFS(0, path, result);
}

void CharacteristicSet::Trie::DFS(uint node_id,
                                  std::vector<uint>& path,
                                  std::vector<std::vector<uint>>& result) {
    if (exist[node_id] > 0) {
        result.push_back(path);
    }

    auto beg = next.lower_bound((ulong)node_id << 32);
    auto end = next.lower_bound((ulong)(node_id + 1) << 32);

    uint next_id;
    for (auto it = beg; it != end; it++) {
        next_id = it->second;
        if (next_id) {
            path.push_back((uint)(it->first & 0xFFFFFFFF) + 1);
            DFS(next_id, path, result);
            path.pop_back();
        }
    }
};

CharacteristicSet::CharacteristicSet() {}

CharacteristicSet::CharacteristicSet(uint cnt) {
    offset_size_ = std::vector<std::pair<uint, uint>>(cnt);
    sets_ = std::vector<std::span<uint>>(cnt);
    base_ = (cnt * 2 + 1) * 4;
}

CharacteristicSet::CharacteristicSet(std::string file_path) : file_path_(file_path) {}

void CharacteristicSet::Load() {
    MMap<uint> c_sets = MMap<uint>(file_path_);
    uint count = c_sets[0];
    base_ = (count * 2 + 1) * 4;
    offset_size_ = std::vector<std::pair<uint, uint>>(count);
    sets_ = std::vector<std::span<uint>>(count);
    mmap_ = MMap<uint8_t>(file_path_);
    for (uint set_id = 1; set_id <= count; set_id++)
        offset_size_[set_id - 1] = {c_sets[2 * set_id - 1], c_sets[2 * set_id]};
    c_sets.CloseMap();
}

void CharacteristicSet::Build(std::vector<std::pair<uint8_t*, uint>>& compressed_sets,
                              std::vector<uint>& original_size) {
    uint base = (compressed_sets.size() * 2 + 1) * 4;

    ulong compressed_size = 0;
    for (uint set_id = 1; set_id <= compressed_sets.size(); set_id++)
        compressed_size += compressed_sets[set_id - 1].second;

    MMap<uint> c_sets_offset_size = MMap<uint>(file_path_, compressed_sets.size() * 4 + compressed_size);
    c_sets_offset_size.Write(compressed_sets.size());
    uint offset = 0;
    for (uint set_id = 1; set_id <= compressed_sets.size(); set_id++) {
        offset += compressed_sets[set_id - 1].second;
        c_sets_offset_size.Write(offset);
        c_sets_offset_size.Write(original_size[set_id - 1]);
    }
    c_sets_offset_size.CloseMap();

    MMap<uint8_t> c_sets = MMap<uint8_t>(file_path_, base + offset);
    offset = 0;
    for (uint set_id = 1; set_id <= compressed_sets.size(); set_id++) {
        for (uint i = 0; i < compressed_sets[set_id - 1].second; i++)
            c_sets[base + offset + i] = compressed_sets[set_id - 1].first[i];
        offset += compressed_sets[set_id - 1].second;
    }
    c_sets.CloseMap();
}

std::span<uint>& CharacteristicSet::operator[](uint c_id) {
    c_id -= 1;
    if (sets_[c_id].size() == 0) {
        uint offset = (c_id == 0) ? 0 : offset_size_[c_id - 1].first;
        uint buffer_size = offset_size_[c_id].first - offset;
        uint original_size = offset_size_[c_id].second;

        uint8_t* compressed_buffer = new uint8_t[buffer_size];
        for (uint i = 0; i < buffer_size; i++)
            compressed_buffer[i] = mmap_[base_ + offset + i];

        uint32_t* original_data = Decompress(compressed_buffer, original_size);
        for (uint i = 1; i < original_size; i++)
            original_data[i] += original_data[i - 1];
        sets_[c_id] = std::span<uint>(original_data, original_size);
    }
    return sets_[c_id];
}
