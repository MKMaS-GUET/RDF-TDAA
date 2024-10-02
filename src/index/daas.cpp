#include "rdf-tdaa/index/daas.hpp"

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

// ones in [begin, end)
uint range_rank(MMap<char>& bits, uint begin, uint end) {
    uint cnt = 0;
    for (uint bit_offset = begin; bit_offset < end; bit_offset++) {
        if (bits[bit_offset / 8] != 0) {
            if (bits[bit_offset / 8] & 1 << (7 - bit_offset % 8))
                cnt++;
        } else {
            bit_offset = bit_offset - bit_offset % 8 + 7;
        }
    }
    return cnt;
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

uint DAAs::CharacteristicSetID(uint id) {
    return AccessToDAA((id - 1) * 2);
}

uint DAAs::DAASize(uint id) {
    return AccessToDAA((id - 1) * 2 + 1);
}

std::pair<uint, uint> DAAs::DAAOffsetSize(uint id) {
    uint daa_offset = 0;
    uint daa_size = AccessToDAA((id - 1) * 2 + 1);  // where next daa start
    if (id != 1) {
        daa_offset = AccessToDAA((id - 2) * 2 + 1);
        daa_size = daa_size - daa_offset;
    }
    return {daa_offset, daa_size};
}

std::shared_ptr<std::vector<uint>> DAAs::AccessDAAAllArrays(uint id) {
    auto [daa_offset, daa_size] = DAAOffsetSize(id);

    ulong bit_start = daa_offset * ulong(daa_levels_width);

    uint uint_base = bit_start / 32;
    uint offset_in_uint = bit_start % 32;

    uint uint_cnt = (offset_in_uint + daa_size * daa_levels_width + 31) / 32;
    uint data = 0;
    uint remaining_bits = daa_levels_width;

    std::vector<uint> levels_mem = std::vector<uint>();

    for (uint uint_offset = uint_base; uint_offset < uint_base + uint_cnt; uint_offset++) {
        uint bits_to_write = std::min(32 - offset_in_uint, remaining_bits);
        uint shift_to_end = 32 - (offset_in_uint + bits_to_write);
        uint mask = ((1ull << bits_to_write) - 1) << shift_to_end;

        uint extracted_bits = (levels_mem[uint_offset] & mask) >> shift_to_end;
        data |= extracted_bits << (remaining_bits - bits_to_write);

        remaining_bits -= bits_to_write;
        offset_in_uint += bits_to_write;
        if (remaining_bits == 0) {
            levels_mem.push_back(data);

            remaining_bits = daa_levels_width;
            data = 0;
            if (offset_in_uint != 32)
                uint_offset--;
        }
        if (offset_in_uint == 32)
            offset_in_uint = 0;
    }

    std::shared_ptr<std::vector<std::vector<uint>>> arrays;

    std::shared_ptr<std::vector<uint>> result = std::make_shared<std::vector<uint>>();

    if (daa_size == 1) {
        result->push_back(levels_mem[0]);
        return result;
    }

    std::vector<uint> level_starts;
    One one = One(daa_level_end, daa_offset, daa_offset + daa_size);
    uint end = one.Next();
    while (end != daa_offset + daa_size) {
        level_starts.push_back(end + 1);
        end = one.Next();
    }

    uint predicate_cnt = level_starts[0] - daa_offset;
    for (uint p = 0; p < predicate_cnt; p++) {
        uint offset = p;
        uint value_cnt = 0;

        uint levels_offset = daa_offset + offset;
        result->push_back(levels_mem[levels_offset - daa_offset]);
        uint level_start = daa_offset;
        while (!get_bit(daa_array_end, levels_offset)) {
            offset = offset - range_rank(daa_array_end, level_start, level_start + offset);

            level_start = level_starts[value_cnt];
            levels_offset = level_start + offset;
            value_cnt++;

            result->push_back(levels_mem[levels_offset - daa_offset] + result->back());
        }
    }

    return result;
}

uint DAAs::AccessToDAA(ulong offset) {
    if (to_daa_compressed_) {
        uint data_width = (offset % 2) ? daa_offset_width : chara_set_id_width;

        ulong bit_start = ulong(offset / 2) * ulong(chara_set_id_width + daa_offset_width) +
                          ((offset % 2) ? chara_set_id_width : 0);

        return AccessBitSequence(to_daa, data_width, bit_start);
    } else {
        return to_daa[offset];
    }
}

uint DAAs::AccessLevels(ulong offset) {
    if (levels_compressed_) {
        ulong bit_start = offset * ulong(daa_levels_width);
        return AccessBitSequence(daa_levels, daa_levels_width, bit_start);
    } else {
        return daa_levels[offset];
    }
}

std::shared_ptr<std::vector<uint>> DAAs::AccessDAA(uint id, uint index) {
    auto [daa_offset, daa_size] = DAAOffsetSize(id);

    uint value;
    uint value_offset;

    value_offset = daa_offset + index;
    value = AccessLevels(value_offset);

    std::shared_ptr<std::vector<uint>> result = std::make_shared<std::vector<uint>>();
    result->push_back(value);

    if (daa_size == 1)
        return result;

    One one = One(daa_level_end, daa_offset, daa_offset + daa_size);

    uint level_start = daa_offset;
    while (!get_bit(daa_array_end, value_offset)) {
        index = index - range_rank(daa_array_end, level_start, level_start + index);

        level_start = one.Next() + 1;
        value_offset = level_start + index;

        value = AccessLevels(value_offset) + result->back();
        result->push_back(value);
    }

    return result;
}