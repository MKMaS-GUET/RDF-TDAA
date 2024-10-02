#include "rdf-tdaa/index/characteristic_set.hpp"
#include "rdf-tdaa/utils/vbyte.hpp"

CharacteristicSet::CharacteristicSet() {}

CharacteristicSet::CharacteristicSet(uint cnt) : count(cnt) {
    offset_size = std::vector<std::pair<uint, uint>>(cnt);
    sets = std::vector<std::vector<uint>>(cnt);
}

std::vector<uint>& CharacteristicSet::operator[](uint c_id) {
    c_id -= 1;
    if (sets[c_id].size() == 0) {
        uint offset = (c_id == 0) ? 0 : offset_size[c_id - 1].first;
        uint buffer_size = offset_size[c_id].first - offset;
        uint original_size = offset_size[c_id].second;

        uint base = (count * 2 + 1) * 4;
        uint8_t* compressed_buffer = new uint8_t[buffer_size];
        for (uint i = 0; i < buffer_size; i++)
            compressed_buffer[i] = mmap[base + offset + i];

        uint32_t* original_data = Decompress(compressed_buffer, original_size);
        for (uint i = 1; i < original_size; i++)
            original_data[i] += original_data[i - 1];
        sets[c_id] = std::vector<uint32_t>(original_data, original_data + original_size);
    }
    return sets[c_id];
}