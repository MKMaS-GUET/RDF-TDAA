#include "rdf-tdaa/index/daas.hpp"
#include <algorithm>
#include <cmath>
#include <iostream>

One::One(MMap<char>& bits, uint begin, uint end) : bits_(bits), bit_offset_(begin), end_(end) {}

// next one in [begin, end)
uint One::Next() {
    for (; bit_offset_ < end_; bit_offset_++) {
        if (bits_[bit_offset_ / 8] != 0) {
            if (bits_[bit_offset_ / 8] & 1 << (7 - bit_offset_ % 8)) {
                bit_offset_++;
                return bit_offset_ - 1;
            }
        } else {
            bit_offset_ = bit_offset_ - bit_offset_ % 8 + 7;
        }
    }
    return end_;
}

// ones in [begin, end]
uint range_rank(MMap<char>& bits, uint begin, uint end) {
    uint cnt = 0;
    for (uint bit_offset = begin; bit_offset <= end; bit_offset++) {
        if (bits[bit_offset / 8] != 0) {
            if (bits[bit_offset / 8] & 1 << (7 - bit_offset % 8))
                cnt++;
        } else {
            bit_offset = bit_offset - bit_offset % 8 + 7;
        }
    }
    return cnt;
}

DAAs::DAA::DAA(std::vector<std::vector<uint>>& arrays) {
    create(arrays);
}

DAAs::DAA::~DAA() {
    delete[] levels;
    delete[] level_end;
    delete[] array_end;
}

void DAAs::DAA::create(std::vector<std::vector<uint>>& arrays) {
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

DAAs::DAAs(Type type) : type_(type) {}

DAAs::DAAs(std::string file_path, Type type) : file_path_(file_path), type_(type) {}

DAAs::DAAs(std::string file_path, Type type, PredicateIndex& predicate_index)
    : file_path_(file_path),
      type_(type),
      predicate_index_p_(std::make_shared<PredicateIndex>(predicate_index)) {}

uint DAAs::EraseAndStatistic(std::vector<uint>& c_set_id,
                             std::vector<std::vector<std::vector<uint>>>& entity_set) {
    uint all_arr_size = 0;
    uint max_c_set_id = 0;
    uint max = 0;
    for (uint id = 1; id <= entity_set.size(); id++) {
        for (uint p = 0; p < entity_set[id - 1].size(); p++) {
            auto& set = entity_set[id - 1][p];
            std::sort(set.begin(), set.end());
            set.erase(std::unique(set.begin(), set.end()), set.end());
            all_arr_size += set.size();

            if (c_set_id[id - 1] > max_c_set_id)
                max_c_set_id = c_set_id[id - 1];

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
    c_set_id_width_ = std::ceil(std::log2(max_c_set_id));

    return all_arr_size;
}

void DAAs::BuildDAAs(std::vector<std::vector<std::vector<uint>>>& entity_set,
                     std::vector<ulong>& daa_offsets) {
    std::string prefix = (type_ == DAAs::Type::kSPO) ? "spo" : "ops";
    ulong entity_cnt = entity_set.size();

    uint one_cnt = 0;
    uint levels_size = 0;
    for (uint id = 1; id <= entity_cnt; id++) {
        if (entity_set[id - 1].size() != 1 || entity_set[id - 1][0].size() != 1) {
            for (uint p = 0; p < entity_set[id - 1].size(); p++)
                levels_size += entity_set[id - 1][p].size();
        } else
            one_cnt++;
    }

    ulong file_size;
    if (compress_levels_)
        file_size = ulong(levels_size * ulong(daa_levels_width_) + 7ul) / 8ul;
    else
        file_size = levels_size * 4;

    daa_levels_ = MMap<uint>(file_path_ + prefix + "_daa_levels", file_size);
    daa_level_end_ = MMap<char>(file_path_ + prefix + "_daa_level_end", ulong(levels_size + 7ul) / 8ul);
    daa_array_end_ = MMap<char>(file_path_ + prefix + "_daa_array_end", ulong(levels_size + 7ul) / 8ul);

    uint daa_file_offset = 0;

    char level_end_buffer = 0;
    char array_end_buffer = 0;
    uint end_buffer_offset = 7;
    uint levels_buffer = 0;
    uint levels_buffer_offset = 31;

    daa_offsets = std::vector<ulong>(entity_cnt, 0);

    for (uint id = 1; id <= entity_cnt; id++) {
        if (entity_set[id - 1].size() != 1 || entity_set[id - 1][0].size() != 1) {
            DAAs::DAA daa = DAAs::DAA(entity_set[id - 1]);

            daa_file_offset += daa.data_cnt;
            daa_offsets[id - 1] = daa_file_offset;

            for (uint levels_offset = 0; levels_offset < daa.data_cnt; levels_offset++) {
                if (compress_levels_) {
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
                } else {
                    daa_levels_.Write(daa.levels[levels_offset]);
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
            daa_offsets[id - 1] = entity_set[id - 1][0][0] | (1ul << (daa_offset_width_ - 1));
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

void DAAs::BuildToDAA(std::vector<uint>& c_set_id, std::vector<ulong>& daa_offsets) {
    std::string prefix = (type_ == DAAs::Type::kSPO) ? "spo" : "ops";
    ulong entity_cnt = c_set_id.size();

    if (compress_to_daa_) {
        ulong file_size =
            ulong(ulong(ulong(ulong(c_set_id_width_) + ulong(daa_offset_width_)) * entity_cnt) + 7ul) / 8ul;
        MMap<uint> to_daa = MMap<uint>(file_path_ + prefix + "_to_daa", file_size);

        ulong to_daa_buffer = 0;
        uint to_daa_buffer_offset = 31;
        for (uint id = 1; id <= entity_cnt; id++) {
            for (uint i = 0; i < c_set_id_width_ + daa_offset_width_; i++) {
                bool bit;
                if (i < c_set_id_width_)
                    bit = c_set_id[id - 1] & (1ull << (c_set_id_width_ - 1 - i));
                else
                    bit = daa_offsets[id - 1] & (1ull << (daa_offset_width_ - 1 - (i - c_set_id_width_)));
                if (bit)
                    to_daa_buffer |= 1ull << to_daa_buffer_offset;

                if (to_daa_buffer_offset == 0) {
                    to_daa.Write(to_daa_buffer);
                    to_daa_buffer = 0;
                    to_daa_buffer_offset = 32;
                }
                to_daa_buffer_offset--;
            }
        }
        if (to_daa_buffer != 0)
            to_daa.Write(to_daa_buffer);

        to_daa.CloseMap();
    } else {
        MMap<uint> to_daa = MMap<uint>(file_path_ + prefix + "_to_daa", ulong(entity_cnt * 2ul * 4ul));
        for (uint id = 1; id <= entity_cnt; id++) {
            to_daa[(id - 1) * 2] = c_set_id[id - 1];
            to_daa[(id - 1) * 2 + 1] = daa_offsets[id - 1];
        }
        to_daa.CloseMap();
    }
}

void DAAs::Build(std::vector<uint>& c_set_id, std::vector<std::vector<std::vector<uint>>>& entity_set) {
    ulong entity_cnt = entity_set.size();

    uint all_set_size = EraseAndStatistic(c_set_id, entity_set);

    daa_offset_width_ = 32;
    if (compress_to_daa_ || compress_levels_) {
        daa_offset_width_ = std::ceil(std::log2(all_set_size));
        // value flags
        daa_offset_width_ += 1;

        MMap<uint> data_width = MMap<uint>(file_path_ + "data_width", 6 * 4);
        data_width[(type_ == DAAs::Type::kSPO) ? 0 : 3] = c_set_id_width_;
        data_width[(type_ == DAAs::Type::kSPO) ? 1 : 4] = daa_offset_width_;
        data_width[(type_ == DAAs::Type::kSPO) ? 2 : 5] = daa_levels_width_;
        data_width.CloseMap();
    }

    std::vector<ulong> daa_offsets(entity_cnt);
    BuildDAAs(entity_set, daa_offsets);
    BuildToDAA(c_set_id, daa_offsets);
}

void DAAs::Load() {
    std::string prefix = (type_ == kSPO) ? "spo_" : "ops_";
    to_daa_ = MMap<uint>(file_path_ + prefix + "to_daa");
    daa_levels_ = MMap<uint>(file_path_ + prefix + "daa_levels");
    daa_level_end_ = MMap<char>(file_path_ + prefix + "daa_level_end");
    daa_array_end_ = MMap<char>(file_path_ + prefix + "daa_array_end");

    subject_characteristic_set_ = CharacteristicSet(file_path_ + "s_c_sets");
    subject_characteristic_set_.Load();
    object_characteristic_set_ = CharacteristicSet(file_path_ + "o_c_sets");
    object_characteristic_set_.Load();

    uint offset = (type_ == kSPO) ? 0 : 3;
    if (compress_to_daa_ || compress_levels_) {
        MMap<uint> data_width = MMap<uint>(file_path_ + "data_width", 6 * 4);
        c_set_id_width_ = data_width[offset];
        daa_offset_width_ = data_width[offset + 1];
        daa_levels_width_ = data_width[offset + 2];
        data_width.CloseMap();
    }
}

uint DAAs::AccessBitSequence(MMap<uint>& bits, uint data_width, ulong bit_start) {
    uint uint_base = bit_start / 32;
    uint offset_in_uint = bit_start % 32;

    uint uint_cnt = (offset_in_uint + data_width + 31) / 32;
    uint data = 0;
    uint remaining_bits = data_width;

    for (uint uint_offset = uint_base; uint_offset < uint_base + uint_cnt; uint_offset++) {
        // 计算可以写入的位数
        uint bits_to_write = std::min(32 - offset_in_uint, remaining_bits);

        // 生成掩码
        uint shift_to_end = 32 - (offset_in_uint + bits_to_write);
        uint mask = ((1ull << bits_to_write) - 1) << shift_to_end;

        // 提取所需位并移位到目标位置
        uint extracted_bits = (bits[uint_offset] & mask) >> shift_to_end;
        data |= extracted_bits << (remaining_bits - bits_to_write);

        remaining_bits -= bits_to_write;
        offset_in_uint = 0;
    }

    return data;
}

std::span<uint>& DAAs::CharacteristicSetOf(uint id) {
    uint c_set_id = AccessToDAA((id - 1) * 2);
    return (type_ == DAAs::Type::kSPO) ? subject_characteristic_set_[c_set_id]
                                       : object_characteristic_set_[c_set_id];
}

uint DAAs::DAASize(uint id) {
    return AccessToDAA((id - 1) * 2 + 1);
}

std::pair<uint, uint> DAAs::DAAOffsetSize(uint id) {
    uint temp = AccessToDAA((id - 1) * 2 + 1);
    if ((temp & (1u << (daa_offset_width_ - 1))) != 0) {
        temp &= ~(1u << (daa_offset_width_ - 1));
        return {temp, 0};
    } else {
        uint daa_offset = 0;
        uint daa_size = temp;  // where next daa start
        if (id != 1) {
            uint i = 1;
            daa_offset = AccessToDAA((id - 1 - i) * 2 + 1);
            while ((daa_offset & (1u << (daa_offset_width_ - 1))) != 0) {
                i++;
                if (id - 1 - i > 0)
                    daa_offset = AccessToDAA((id - 1 - i) * 2 + 1);
                else
                    break;
            }
            daa_size = daa_size - daa_offset;
        }
        return {daa_offset, daa_size};
    }
}

std::span<uint> DAAs::AccessDAAAllArrays(uint id) {
    auto [daa_offset, daa_size] = DAAOffsetSize(id);

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

    auto& c_set = CharacteristicSetOf(id);
    std::vector<uint>* result = new std::vector<uint>();

    if (daa_size == 1) {
        std::span<uint>& offset2id = (type_ == DAAs::Type::kSPO) ? predicate_index_p_->GetOSet(c_set[0])
                                                                 : predicate_index_p_->GetSSet(c_set[0]);
        result->push_back(offset2id[levels_mem[0]]);
        return std::span<uint>(*result);
    }

    std::vector<uint> level_starts;
    One one = One(daa_level_end_, daa_offset, daa_offset + daa_size);
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

        std::span<uint>& offset2id = (type_ == DAAs::Type::kSPO) ? predicate_index_p_->GetOSet(c_set[p])
                                                                 : predicate_index_p_->GetSSet(c_set[p]);

        uint levels_offset = daa_offset + offset;
        result->push_back(levels_mem[levels_offset - daa_offset]);
        uint level_start = daa_offset;
        while (!bit_get(daa_array_end_, levels_offset)) {
            offset = offset - range_rank(daa_array_end_, level_start, level_start + offset);

            level_start = level_starts[value_cnt];
            levels_offset = level_start + offset;
            value_cnt++;

            result->push_back(levels_mem[levels_offset - daa_offset] + result->back());
        }
        for (uint i = start_offset; i < result->size(); i++)
            result->operator[](i) = offset2id[result->at(i)];
    }

    return std::span<uint>(*result);
}

uint DAAs::AccessToDAA(ulong offset) {
    if (compress_to_daa_) {
        uint data_width = (offset % 2) ? daa_offset_width_ : c_set_id_width_;

        ulong bit_start = ulong(offset / 2) * ulong(c_set_id_width_ + daa_offset_width_) +
                          ((offset % 2) ? c_set_id_width_ : 0);

        return AccessBitSequence(to_daa_, data_width, bit_start);
    } else {
        return to_daa_[offset];
    }
}

uint DAAs::AccessLevels(ulong offset) {
    if (compress_levels_) {
        ulong bit_start = offset * ulong(daa_levels_width_);
        return AccessBitSequence(daa_levels_, daa_levels_width_, bit_start);
    } else {
        return daa_levels_[offset];
    }
}

std::span<uint> DAAs::AccessDAA(uint id, uint pid, uint index) {
    auto [daa_offset, daa_size] = DAAOffsetSize(id);

    std::span<uint>& offset2id =
        (type_ == DAAs::Type::kSPO) ? predicate_index_p_->GetOSet(pid) : predicate_index_p_->GetSSet(pid);
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

    One one = One(daa_level_end_, daa_offset, daa_offset + daa_size);
    if (daa_size == 1) {
        result->operator[](0) = offset2id[result->at(0)];
        return std::span<uint>(*result);
    }

    uint level_start = daa_offset;
    while (!bit_get(daa_array_end_, value_offset)) {
        index = index - range_rank(daa_array_end_, level_start, level_start + index);

        level_start = one.Next() + 1;
        value_offset = level_start + index;

        value = AccessLevels(value_offset) + result->back();
        result->push_back(value);
    }

    for (uint i = 0; i < result->size(); i++)
        result->operator[](i) = offset2id[result->at(i)];

    return std::span<uint>(*result);
}

void DAAs::Close() {
    to_daa_.CloseMap();
    daa_levels_.CloseMap();
    daa_level_end_.CloseMap();
    daa_array_end_.CloseMap();
}