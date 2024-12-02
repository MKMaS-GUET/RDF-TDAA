#include "rdf-tdaa/index/cs_daa_map.hpp"
#include <algorithm>
#include <cmath>
#include <iostream>
#include "rdf-tdaa/utils/bit_operations.hpp"

CsDaaMap::CsDaaMap(std::string file_path) : file_path_(file_path) {
    cs_id_width_ = std::pair<uint, uint>(0, 0);
    daa_offset_width_ = std::pair<uint, uint>(0, 0);
}

CsDaaMap::CsDaaMap(std::string file_path,
                   std::pair<uint, uint> cs_id_width,
                   std::pair<uint, uint> daa_offset_width,
                   uint not_shared_cs_id_width,
                   uint not_shared_daa_offset_width,
                   uint shared_cnt,
                   uint subject_cnt,
                   uint object_cnt,
                   uint shared_id_size)
    : file_path_(file_path),
      cs_id_width_(cs_id_width),
      daa_offset_width_(daa_offset_width),
      not_shared_cs_id_width_(not_shared_cs_id_width),
      not_shared_daa_offset_width_(not_shared_daa_offset_width),
      shared_cnt_(shared_cnt),
      subject_cnt_(subject_cnt),
      object_cnt_(object_cnt),
      shared_id_size_(shared_id_size) {
    cs_daa_map_ = MMap<uint>(file_path_);
    shared_width_ =
        cs_id_width_.first + daa_offset_width_.first + cs_id_width_.second + daa_offset_width_.second;
    not_shared_width_ = not_shared_cs_id_width + not_shared_daa_offset_width;
}

void CsDaaMap::Build(std::pair<std::vector<uint>&, std::vector<ulong>&> spo_map,
                     std::pair<std::vector<uint>&, std::vector<ulong>&> ops_map) {
    cs_id_width_.first = std::ceil(std::log2(*std::max_element(spo_map.first.begin(), spo_map.first.end())));
    cs_id_width_.second = std::ceil(std::log2(*std::max_element(ops_map.first.begin(), ops_map.first.end())));
    daa_offset_width_.first =
        std::ceil(std::log2(*std::max_element(spo_map.second.begin(), spo_map.second.end())));
    daa_offset_width_.second =
        std::ceil(std::log2(*std::max_element(ops_map.second.begin(), ops_map.second.end())));

    not_shared_cs_id_width_ = cs_id_width_.second;
    not_shared_daa_offset_width_ = daa_offset_width_.second;
    shared_id_size_ = spo_map.first.size();
    if (ops_map.first.size() < shared_id_size_) {
        shared_id_size_ = ops_map.first.size();
        not_shared_cs_id_width_ = cs_id_width_.first;
        not_shared_daa_offset_width_ = daa_offset_width_.first;
    }
    ulong not_shared_size = std::abs(static_cast<int>(spo_map.first.size() - ops_map.first.size()));
    uint not_shared_width = not_shared_cs_id_width_ + not_shared_daa_offset_width_;

    ulong spo_width = ulong(cs_id_width_.first + daa_offset_width_.first);
    ulong ops_width = ulong(cs_id_width_.second + daa_offset_width_.second);
    ulong file_size =
        (shared_id_size_ * (spo_width + ops_width) + not_shared_size * not_shared_width + 7ul) / 8ul;

    cs_daa_map_ = MMap<uint>(file_path_ + "cs_daa_map", file_size);

    ulong buffer = 0;
    uint buffer_offset = 31;
    for (uint id = 1; id <= shared_id_size_; id++) {
        for (uint i = 0; i < spo_width + ops_width; i++) {
            bool bit;
            if (i < spo_width) {
                if (i < cs_id_width_.first)  // [0, spo_cs_id_width)
                    bit = spo_map.first[id - 1] & (1ull << (cs_id_width_.first - 1 - i));
                else  // [spo_cs_id_width, spo_cs_id_width + spo_daa_offset_width)
                    bit = spo_map.second[id - 1] &
                          (1ull << (daa_offset_width_.first - 1 - (i - cs_id_width_.first)));
            } else {
                uint idx = i - (cs_id_width_.first + daa_offset_width_.first);
                if (idx < cs_id_width_.second)
                    // [spo_cs_id_width + spo_daa_offset_width,
                    // spo_cs_id_width + spo_daa_offset_width + ops_cs_id_width)
                    bit = ops_map.first[id - 1] & (1ull << (cs_id_width_.second - 1 - idx));
                else
                    // [spo_cs_id_width + spo_daa_offset_width + ops_cs_id_width,
                    // spo_cs_id_width + spo_daa_offset_width + ops_cs_id_width + ops_daa_offset_width)
                    bit = ops_map.second[id - 1] &
                          (1ull << (daa_offset_width_.second - 1 - (idx - cs_id_width_.second)));
            }

            if (bit)
                buffer |= 1ull << buffer_offset;

            if (buffer_offset == 0) {
                cs_daa_map_.Write(buffer);
                buffer = 0;
                buffer_offset = 32;
            }
            buffer_offset--;
        }
    }
    std::pair<std::vector<uint>&, std::vector<ulong>&>& larger_map =
        (spo_map.first.size() > ops_map.first.size()) ? spo_map : ops_map;
    ulong larger_size = larger_map.first.size();
    for (uint id = shared_id_size_ + 1; id <= larger_size; id++) {
        for (uint i = 0; i < not_shared_width; i++) {
            bool bit;
            if (i < not_shared_cs_id_width_)  // [0, not_shared_cs_id_width)
                bit = larger_map.first[id - 1] & (1ull << (not_shared_cs_id_width_ - 1 - i));
            else  // [not_shared_cs_id_width, not_shared_cs_id_width + not_shared_daa_offset_width)
                bit = larger_map.second[id - 1] &
                      (1ull << (not_shared_daa_offset_width_ - 1 - (i - not_shared_cs_id_width_)));
            if (bit)
                buffer |= 1ull << buffer_offset;

            if (buffer_offset == 0) {
                cs_daa_map_.Write(buffer);
                buffer = 0;
                buffer_offset = 32;
            }
            buffer_offset--;
        }
    }
    if (buffer != 0)
        cs_daa_map_.Write(buffer);

    cs_daa_map_.CloseMap();
}

uint CsDaaMap::ChararisticSetIdOf(uint id, Permutation permutation) {
    if (permutation == Permutation::kOPS && id > shared_cnt_)
        id -= subject_cnt_;

    ulong bit_start = 0;
    uint access_width = 0;
    if (id <= shared_id_size_) {
        if (permutation == Permutation::kSPO) {
            bit_start = (ulong(id) - 1) * shared_width_;
            access_width = cs_id_width_.first;
        }
        if (permutation == Permutation::kOPS) {
            bit_start = (ulong(id) - 1) * shared_width_ + (cs_id_width_.first + daa_offset_width_.first);
            access_width = cs_id_width_.second;
        }
    } else {
        bit_start = shared_id_size_ * shared_width_ + (ulong(id) - shared_id_size_ - 1) * not_shared_width_;
        access_width = not_shared_cs_id_width_;
    }

    return bitop::AccessBitSequence(cs_daa_map_, bit_start, access_width);
}

uint CsDaaMap::DAAOffsetOf(uint id, Permutation permutation) {
    if (permutation == Permutation::kOPS && id > shared_cnt_)
        id -= subject_cnt_;

    ulong bit_start = 0;
    uint access_width = 0;
    if (id <= shared_id_size_) {
        if (permutation == Permutation::kSPO) {
            bit_start = (ulong(id) - 1) * shared_width_ + cs_id_width_.first;
            access_width = daa_offset_width_.first;
        }
        if (permutation == Permutation::kOPS) {
            bit_start = (ulong(id) - 1) * shared_width_ + (cs_id_width_.first + daa_offset_width_.first) +
                        cs_id_width_.second;
            access_width = daa_offset_width_.second;
        }
    } else {
        bit_start = shared_id_size_ * shared_width_ + (ulong(id) - shared_id_size_ - 1) * not_shared_width_ +
                    cs_id_width_.second;
        access_width = not_shared_daa_offset_width_;
    }

    return bitop::AccessBitSequence(cs_daa_map_, bit_start, access_width);
}

std::pair<uint, uint> CsDaaMap::DAAOffsetSizeOf(uint id, Permutation permutation) {
    uint end = DAAOffsetOf(id, permutation);
    uint daa_offset_width =
        (permutation == Permutation::kSPO) ? daa_offset_width_.first : daa_offset_width_.second;

    if ((end & (1u << (daa_offset_width - 1))) != 0) {
        end &= ~(1u << (daa_offset_width - 1));
        return {end, 0};
    }

    // offset in the offset array is the start of the next DAA
    uint daa_offset = 0;
    uint daa_size = end;  // where next daa start
    if (id != 1) {
        uint i = 1;
        daa_offset = DAAOffsetOf(id - i, permutation);
        while ((daa_offset & (1u << (daa_offset_width - 1))) != 0) {
            i++;
            if (id - i > 0)
                daa_offset = DAAOffsetOf(id - i, permutation);
            else
                break;
        }
        daa_size = daa_size - daa_offset;
    }
    return {daa_offset, daa_size};
}

uint CsDaaMap::shared_id_size() {
    return shared_id_size_;
}

std::pair<uint, uint> CsDaaMap::cs_id_width() {
    return cs_id_width_;
}

std::pair<uint, uint> CsDaaMap::daa_offset_width() {
    return daa_offset_width_;
}

uint CsDaaMap::not_shared_cs_id_width() {
    return not_shared_cs_id_width_;
}

uint CsDaaMap::not_shared_daa_offset_width() {
    return not_shared_daa_offset_width_;
}
