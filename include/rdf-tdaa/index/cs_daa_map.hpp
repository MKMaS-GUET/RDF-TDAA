#ifndef CS_DAA_MAP_HPP
#define CS_DAA_MAP_HPP

#include <sys/types.h>
#include <string>
#include <vector>
#include "rdf-tdaa/utils/mmap.hpp"

class CsDaaMap {
   public:
    enum Permutation { kSPO, kOPS };

   private:
    std::string file_path_;
    MMap<uint> cs_daa_map_;

    std::pair<uint, uint> cs_id_width_;
    std::pair<uint, uint> daa_offset_width_;
    uint shared_width_;
    uint not_shared_width_;
    uint not_shared_cs_id_width_;
    uint not_shared_daa_offset_width_;
    uint shared_cnt_;
    uint subject_cnt_;
    uint object_cnt_;
    uint shared_id_size_;

    uint DAAOffsetOf(uint id, Permutation permutation);

   public:
    CsDaaMap() = default;

    CsDaaMap(std::string file_path);

    CsDaaMap(std::string file_path,
             std::pair<uint, uint> cs_id_width,
             std::pair<uint, uint> daa_offset_width,
             uint not_shared_cs_id_width,
             uint not_shared_daa_offset_width,
             uint shared_cnt,
             uint subject_cnt,
             uint object_cnt,
             uint shared_id_size);

    void Build(std::pair<std::vector<uint>&, std::vector<ulong>&> spo_map,
               std::pair<std::vector<uint>&, std::vector<ulong>&> ops_map);

    uint ChararisticSetIdOf(uint id, Permutation permutation);
    std::pair<uint, uint> DAAOffsetSizeOf(uint id, Permutation permutation);

    uint shared_id_size();
    std::pair<uint, uint> cs_id_width();
    std::pair<uint, uint> daa_offset_width();
    uint not_shared_cs_id_width();
    uint not_shared_daa_offset_width();
};

#endif