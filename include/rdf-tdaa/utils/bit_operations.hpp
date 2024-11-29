#ifndef BIT_OPERATION_HPP
#define BIT_OPERATION_HPP

#include "rdf-tdaa/utils/mmap.hpp"

namespace bitop {
#define bit_set(bits, offset) ((bits)[(offset) / 8] |= (1 << (7 - (offset) % 8)))
#define bit_get(bits, offset) (((bits)[(offset) / 8] >> (7 - (offset) % 8)) & 1)

class One {
    MMap<char>& bits_;

    uint bit_offset_;
    uint end_;

   public:
    One(MMap<char>& bits, uint begin, uint end);

    // next one in [begin, end)
    uint Next();
};

// ones in [begin, end)
uint range_rank(MMap<char>& bits, uint begin, uint end);

uint AccessBitSequence(MMap<uint>& bits, ulong bit_start, uint data_width);
}  // namespace bitop

#endif