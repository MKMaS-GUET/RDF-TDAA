#ifndef DAAS_HPP
#define DAAS_HPP

#include <memory>
#include <span>
#include <vector>
#include "rdf-tdaa/index/characteristic_set.hpp"
#include "rdf-tdaa/index/predicate_index.hpp"
#include "rdf-tdaa/utils/mmap.hpp"

class DAAs {
   public:
    struct Structure {
        uint data_cnt;
        uint* levels;
        char* level_end;
        char* array_end;

        void create(std::vector<std::vector<uint>>& arrays);

       public:
        Structure(std::vector<std::vector<uint>>& arrays);
        ~Structure();
    };

   private:
    std::string file_path_;

    std::vector<ulong> daa_offsets_;

    uint daa_levels_width_;
    MMap<uint> daa_levels_;
    MMap<char> daa_level_end_;
    MMap<char> daa_array_end_;

    void Preprocess(std::vector<std::vector<std::vector<uint>>>& entity_set);

    void BuildDAAs(std::vector<std::vector<std::vector<uint>>>& entity_set);

   public:
    DAAs();
    DAAs(std::string file_path);
    DAAs(std::string file_path, uint daa_levels_width);

    void Build(std::vector<std::vector<std::vector<uint>>>& entity_set);

    std::vector<ulong>& daa_offsets();

    void Load();

    uint AccessLevels(ulong offset);

    std::span<uint> AccessDAA(uint daa_offset, uint daa_size, std::span<uint>& offset2id, uint index);

    std::span<uint> AccessDAAAllArrays(uint daa_offset,
                                       uint daa_size,
                                       std::vector<std::span<uint>>& offset2id);

    uint daa_levels_width();

    void Close();
};

#endif