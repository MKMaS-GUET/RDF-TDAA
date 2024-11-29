#include "rdf-tdaa/index/daas.hpp"
#include <algorithm>
#include <cmath>
#include <iostream>
#include "rdf-tdaa/utils/bit_operations.hpp"

DAAs::Structure::Structure(std::vector<std::vector<uint>>& arrays) {
    create(arrays);
}

DAAs::Structure::~Structure() {
    delete[] levels;
    delete[] level_end;
    delete[] array_end;
}

void DAAs::Structure::create(std::vector<std::vector<uint>>& arrays) {
    uint level_cnt = 0;
    for (auto& array : arrays) {
        if (array.size() > level_cnt)
            level_cnt = array.size();
    }

    uint* level_size = (uint*)malloc(sizeof(uint) * level_cnt);
    for (uint i = 0; i < level_cnt; i++)
        level_size[i] = 0;

    data_cnt = 0;
    for (uint i = 0; i < arrays.size(); i++) {
        for (uint j = 0; j < arrays[i].size(); j++) {
            level_size[j]++;
            data_cnt++;
        }
    }

    level_end = (char*)malloc((data_cnt + 7) / 8);
    levels = (uint*)malloc(sizeof(uint) * data_cnt);
    array_end = (char*)malloc((data_cnt + 7) / 8);
    uint* contB = (uint*)malloc(sizeof(uint) * level_cnt + 1);

    for (uint i = 0; i < (data_cnt + 7) / 8; i++) {
        level_end[i] = 0;
        array_end[i] = 0;
    }

    contB[0] = 0;
    for (uint j = 0; j < level_cnt; j++) {
        contB[j + 1] = contB[j] + level_size[j];
        bit_set(level_end, contB[j + 1] - 1);
    }

    uint top_level;
    for (uint i = 0; i < arrays.size(); i++) {
        top_level = arrays[i].size() - 1;
        for (uint j = 0; j <= top_level; j++) {
            levels[contB[j]] = arrays[i][j];
            contB[j]++;
        }
        bit_set(array_end, contB[top_level] - 1);
    }

    free(contB);
    free(level_size);
}

DAAs::DAAs() {}

DAAs::DAAs(std::string file_path) : file_path_(file_path) {}

DAAs::DAAs(std::string file_path, uint daa_levels_width)
    : file_path_(file_path), daa_levels_width_(daa_levels_width) {}

void DAAs::Preprocess(std::vector<std::vector<std::vector<uint>>>& entity_set) {
    uint max = 0;
    for (uint id = 1; id <= entity_set.size(); id++) {
        for (uint p = 0; p < entity_set[id - 1].size(); p++) {
            auto& set = entity_set[id - 1][p];
            std::sort(set.begin(), set.end());
            set.erase(std::unique(set.begin(), set.end()), set.end());

            if (set[0] > max)
                max = set[0];
            uint last = 0;
            for (uint i = 0; i < set.size() - 1; i++) {
                last += set[i];
                set[i + 1] -= last;
                if (set[i + 1] > max)
                    max = set[i + 1];
            }
        }
    }

    daa_levels_width_ = std::ceil(std::log2(max));
}

void DAAs::BuildDAAs(std::vector<std::vector<std::vector<uint>>>& entity_set) {
    ulong entity_cnt = entity_set.size();

    uint levels_size = 0;
    for (uint id = 1; id <= entity_cnt; id++) {
        if (entity_set[id - 1].size() != 1 || entity_set[id - 1][0].size() != 1) {
            for (uint p = 0; p < entity_set[id - 1].size(); p++)
                levels_size += entity_set[id - 1][p].size();
        }
    }
    uint daa_offset_width = std::ceil(std::log2(levels_size)) + 1;

    ulong file_size;
    file_size = ulong(levels_size * ulong(daa_levels_width_) + 7ul) / 8ul;

    daa_levels_ = MMap<uint>(file_path_ + "daa_levels", file_size);
    daa_level_end_ = MMap<char>(file_path_ + "daa_level_end", ulong(levels_size + 7ul) / 8ul);
    daa_array_end_ = MMap<char>(file_path_ + "daa_array_end", ulong(levels_size + 7ul) / 8ul);

    uint daa_file_offset = 0;

    char level_end_buffer = 0;
    char array_end_buffer = 0;
    uint end_buffer_offset = 7;
    uint levels_buffer = 0;
    uint levels_buffer_offset = 31;

    daa_offsets_ = std::vector<ulong>(entity_cnt, 0);

    for (uint id = 1; id <= entity_cnt; id++) {
        if (entity_set[id - 1].size() != 1 || entity_set[id - 1][0].size() != 1) {
            DAAs::Structure daa = DAAs::Structure(entity_set[id - 1]);

            daa_file_offset += daa.data_cnt;
            daa_offsets_[id - 1] = daa_file_offset;

            for (uint levels_offset = 0; levels_offset < daa.data_cnt; levels_offset++) {
                for (uint i = 0; i < daa_levels_width_; i++) {
                    if (daa.levels[levels_offset] & (1 << (daa_levels_width_ - 1 - i)))
                        levels_buffer |= 1 << levels_buffer_offset;
                    if (levels_buffer_offset == 0) {
                        daa_levels_.Write(levels_buffer);
                        levels_buffer = 0;
                        levels_buffer_offset = 32;
                    }
                    levels_buffer_offset--;
                }

                if (daa.level_end[levels_offset / 8] & (1 << (7 - levels_offset % 8)))
                    level_end_buffer |= 1 << end_buffer_offset;
                if (daa.array_end[levels_offset / 8] & (1 << (7 - levels_offset % 8)))
                    array_end_buffer |= 1 << end_buffer_offset;
                if (end_buffer_offset == 0) {
                    daa_level_end_.Write(level_end_buffer);
                    daa_array_end_.Write(array_end_buffer);
                    level_end_buffer = 0;
                    array_end_buffer = 0;
                    end_buffer_offset = 8;
                }
                end_buffer_offset--;
            }
        } else {
            daa_offsets_[id - 1] = entity_set[id - 1][0][0] | (1ul << (daa_offset_width - 1));
        }
    }
    if (levels_buffer != 0)
        daa_levels_.Write(levels_buffer);
    if (level_end_buffer != 0) {
        daa_level_end_.Write(level_end_buffer);
        daa_array_end_.Write(array_end_buffer);
    }

    daa_levels_.CloseMap();
    daa_level_end_.CloseMap();
    daa_array_end_.CloseMap();
}

void DAAs::Build(std::vector<std::vector<std::vector<uint>>>& entity_set) {
    Preprocess(entity_set);
    BuildDAAs(entity_set);
}

std::vector<ulong>& DAAs::daa_offsets() {
    return daa_offsets_;
}

void DAAs::Load() {
    daa_levels_ = MMap<uint>(file_path_ + "daa_levels");
    daa_level_end_ = MMap<char>(file_path_ + "daa_level_end");
    daa_array_end_ = MMap<char>(file_path_ + "daa_array_end");
}

std::span<uint> DAAs::AccessDAAAllArrays(uint daa_offset,
                                         uint daa_size,
                                         std::vector<std::span<uint>>& offset2id) {
    ulong bit_start = daa_offset * ulong(daa_levels_width_);

    uint uint_base = bit_start / 32;
    uint offset_in_uint = bit_start % 32;

    uint uint_cnt = (offset_in_uint + daa_size * daa_levels_width_ + 31) / 32;
    uint data = 0;
    uint remaining_bits = daa_levels_width_;

    std::vector<uint> levels_mem = std::vector<uint>();

    for (uint uint_offset = uint_base; uint_offset < uint_base + uint_cnt; uint_offset++) {
        uint bits_to_write = std::min(32 - offset_in_uint, remaining_bits);
        uint shift_to_end = 32 - (offset_in_uint + bits_to_write);
        uint mask = ((1ull << bits_to_write) - 1) << shift_to_end;

        uint extracted_bits = (daa_levels_[uint_offset] & mask) >> shift_to_end;
        data |= extracted_bits << (remaining_bits - bits_to_write);

        remaining_bits -= bits_to_write;
        offset_in_uint += bits_to_write;
        if (remaining_bits == 0) {
            levels_mem.push_back(data);

            remaining_bits = daa_levels_width_;
            data = 0;
            if (offset_in_uint != 32)
                uint_offset--;
        }
        if (offset_in_uint == 32)
            offset_in_uint = 0;
    }

    std::vector<uint>* result = new std::vector<uint>();

    if (daa_size == 1) {
        result->push_back(offset2id[0][levels_mem[0]]);
        return std::span<uint>(*result);
    }

    std::vector<uint> level_starts;
    bitop::One one = bitop::One(daa_level_end_, daa_offset, daa_offset + daa_size);
    uint end = one.Next();
    while (end != daa_offset + daa_size) {
        level_starts.push_back(end + 1);
        end = one.Next();
    }

    uint predicate_cnt = level_starts[0] - daa_offset;
    for (uint p = 0; p < predicate_cnt; p++) {
        uint offset = p;
        uint value_cnt = 0;
        uint start_offset = result->size();

        uint levels_offset = daa_offset + offset;
        result->push_back(levels_mem[levels_offset - daa_offset]);
        uint level_start = daa_offset;
        while (!bit_get(daa_array_end_, levels_offset)) {
            offset = offset - bitop::range_rank(daa_array_end_, level_start, level_start + offset);

            level_start = level_starts[value_cnt];
            levels_offset = level_start + offset;
            value_cnt++;

            result->push_back(levels_mem[levels_offset - daa_offset] + result->back());
        }
        for (uint i = start_offset; i < result->size(); i++)
            result->operator[](i) = offset2id[p][result->at(i)];
    }

    return std::span<uint>(*result);
}

uint DAAs::AccessLevels(ulong offset) {
    ulong bit_start = offset * ulong(daa_levels_width_);
    return bitop::AccessBitSequence(daa_levels_, bit_start, daa_levels_width_);
}

std::span<uint> DAAs::AccessDAA(uint daa_offset, uint daa_size, std::span<uint>& offset2id, uint index) {
    std::vector<uint>* result = new std::vector<uint>;

    if (daa_size == 0) {
        result->push_back(offset2id[daa_offset]);
        return std::span<uint>(*result);
    }

    uint value;
    uint value_offset;

    value_offset = daa_offset + index;
    value = AccessLevels(value_offset);
    result->push_back(value);

    bitop::One one = bitop::One(daa_level_end_, daa_offset, daa_offset + daa_size);
    if (daa_size == 1) {
        result->operator[](0) = offset2id[result->at(0)];
        return std::span<uint>(*result);
    }

    uint level_start = daa_offset;
    while (!bit_get(daa_array_end_, value_offset)) {
        index = index - bitop::range_rank(daa_array_end_, level_start, level_start + index);

        level_start = one.Next() + 1;
        value_offset = level_start + index;

        value = AccessLevels(value_offset) + result->back();
        result->push_back(value);
    }

    for (uint i = 0; i < result->size(); i++)
        result->operator[](i) = offset2id[result->at(i)];

    return std::span<uint>(*result);
}

uint DAAs::daa_levels_width() {
    return daa_levels_width_;
}

void DAAs::Close() {
    daa_levels_.CloseMap();
    daa_level_end_.CloseMap();
    daa_array_end_.CloseMap();
}