#ifndef CHARACTERISTIC_SET_HPP
#define CHARACTERISTIC_SET_HPP

#include <vector>
#include "rdf-tdaa/utils/mmap.hpp"
#include <span>

struct CharacteristicSet {
    uint count;
    MMap<uint8_t> mmap;
    std::vector<std::pair<uint, uint>> offset_size;
    std::vector<std::span<uint>> sets;

    CharacteristicSet();
    CharacteristicSet(uint cnt);

    std::span<uint>& operator[](uint c_id);
};

#endif