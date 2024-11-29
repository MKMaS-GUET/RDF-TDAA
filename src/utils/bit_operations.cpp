#include "rdf-tdaa/utils/bit_operations.hpp"
#include <iostream>

namespace bitop {

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

uint AccessBitSequence(MMap<uint>& bits, ulong bit_start, uint data_width) {
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
}  // namespace bitop
