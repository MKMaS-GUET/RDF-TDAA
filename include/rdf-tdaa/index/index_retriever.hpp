#ifndef INDEX_RETRIEVER_HPP
#define INDEX_RETRIEVER_HPP

#include <limits.h>
#include <fstream>
#include <iostream>
#include <thread>
#include <vector>
#include "rdf-tdaa/dictionary/dictionary.hpp"
#include "rdf-tdaa/utils/mmap.hpp"
#include "rdf-tdaa/utils/result_list.hpp"
#include "streamvbyte.h"

using Result = ResultList::Result;

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

class IndexRetriever {
    struct DAA {
        uint chara_set_id_width_;
        uint daa_offset_width_;
        uint daa_levels_width_;
        MMap<uint> to_daa_;
        MMap<uint> daa_levels_;
        MMap<char> daa_level_end_;
        MMap<char> daa_array_end_;
    };

    std::string db_name_;
    std::string db_dictionary_path_;
    std::string db_index_path_;

    bool predicate_index_compressed_ = true;
    bool to_daa_compressed_ = true;
    bool levels_compressed_ = true;

    Dictionary dict_;

    DAA spo_;
    DAA ops_;

    ulong predicate_index_arrays_file_size_;
    MMap<uint> predicate_index_;
    MMap<uint> predicate_index_arrays_no_compress_;
    MMap<uint8_t> predicate_index_arrays_;
    std::vector<std::shared_ptr<Result>> ps_sets_;
    std::vector<std::shared_ptr<Result>> po_sets_;

    std::vector<std::vector<uint>> subject_characteristic_set_;
    std::vector<std::vector<uint>> object_characteristic_set_;

    ulong FileSize(std::string file_name);

    void Init();

    void LoadAndDecompress(std::vector<std::vector<uint>>& predicate_sets, std::string filename);

    uint AccessBitSequence(MMap<uint>& bits, uint data_width, ulong bit_start);

    uint AccessToDAA(DAA& daa, ulong offset);

    uint AccessLevels(ulong offset, Order order);

    std::shared_ptr<Result> AccessDAA(uint offset, uint daa_start, uint daa_size, Order order);

   public:
    IndexRetriever();

    IndexRetriever(std::string db_name);

    void Close();

    const std::string& ID2String(uint id, Pos pos);

    uint String2ID(const std::string& str, Pos pos);

    uint triplet_cnt();

    uint predicate_cnt();

    std::shared_ptr<Result> GetSSet(uint pid);

    uint GetSSetSize(uint pid);

    std::shared_ptr<Result> GetOSet(uint pid);

    uint GetOSetSize(uint pid);

    uint PSSize(uint pid);

    uint POSize(uint pid);

    std::shared_ptr<Result> GetBySP(uint s, uint p);

    std::shared_ptr<Result> GetByOP(uint o, uint p);

    uint GetBySPSize(uint s, uint p);

    uint GetByOPSize(uint o, uint p);
};

#endif