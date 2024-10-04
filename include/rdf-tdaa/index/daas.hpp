#ifndef DAAS_HPP
#define DAAS_HPP

#include <memory>
#include <span>
#include <vector>
#include "rdf-tdaa/utils/mmap.hpp"

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

class DAAs {
   public:
    enum Type { kSPO, kOPS };

    struct DAA {
        uint data_cnt;
        uint* levels;
        char* level_end;
        char* array_end;

        void create(std::vector<std::vector<uint>>& arrays);

       public:
        DAA(std::vector<std::vector<uint>>& arrays);
        ~DAA();
    };

   private:
    uint AccessBitSequence(MMap<uint>& bits, uint data_width, ulong bit_start);

    uint AccessToDAA(ulong offset);

    std::pair<uint, uint> DAAOffsetSize(uint id);

    bool compress_to_daa_ = true;
    bool compress_levels_ = true;

    DAAs::Type type_;
    std::string file_path_;
    uint c_set_id_width_;
    uint daa_offset_width_;
    uint daa_levels_width_;
    MMap<uint> to_daa_;
    MMap<uint> daa_levels_;
    MMap<char> daa_level_end_;
    MMap<char> daa_array_end_;

    uint EraseAndStatistic(std::vector<uint>& c_set_id,
                           std::vector<std::vector<std::vector<uint>>>& entity_set);

    void BuildDAAs(std::vector<std::vector<std::vector<uint>>>& entity_set,
                   std::vector<uint>& daa_offsets,
                   uint all_arr_size);

    void BuildToDAA(std::vector<uint>& c_set_id, std::vector<uint>& daa_offsets);

   public:
    DAAs();

    DAAs(std::string file_path, Type type);

    DAAs(Type type);

    void Build(std::vector<uint>& c_set_id, std::vector<std::vector<std::vector<uint>>>& entity_set);

    void Load();

    uint CharacteristicSetID(uint id);

    uint DAASize(uint id);

    uint AccessLevels(ulong offset);

    std::span<uint> AccessDAA(uint id, uint offset);

    std::span<uint> AccessDAAAllArrays(uint id);

    void Close();
};

#endif