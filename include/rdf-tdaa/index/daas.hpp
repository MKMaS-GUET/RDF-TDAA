#ifndef DAAS_HPP
#define DAAS_HPP

#include <memory>
#include <vector>
#include "rdf-tdaa/utils/mmap.hpp"

#define get_bit(bits, offset) ((((bits)[(offset) / 8] >> (7 - (offset) % 8)) & 1))

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

class DAAs {
    uint AccessBitSequence(MMap<uint>& bits, uint data_width, ulong bit_start);

    uint AccessToDAA(ulong offset);

    std::pair<uint, uint> DAAOffsetSize(uint id);

   public:
    bool to_daa_compressed_ = true;
    bool levels_compressed_ = true;

    uint chara_set_id_width;
    uint daa_offset_width;
    uint daa_levels_width;
    MMap<uint> to_daa;
    MMap<uint> daa_levels;
    MMap<char> daa_level_end;
    MMap<char> daa_array_end;

    uint CharacteristicSetID(uint id);

    uint DAASize(uint id);

    uint AccessLevels(ulong offset);

    std::shared_ptr<std::vector<uint>> AccessDAA(uint id, uint offset);

    std::shared_ptr<std::vector<uint>> AccessDAAAllArrays(uint id);
};

#endif