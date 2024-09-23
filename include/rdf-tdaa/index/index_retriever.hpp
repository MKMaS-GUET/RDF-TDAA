#ifndef INDEX_RETRIEVER_HPP
#define INDEX_RETRIEVER_HPP

#include <limits.h>
#include <fstream>
#include <iostream>
#include <thread>
#include <vector>
#include "rdf-tdaa/dictionary/dictionary.hpp"
#include "rdf-tdaa/utils/join_list.hpp"
#include "rdf-tdaa/utils/mmap.hpp"
#include "streamvbyte.h"

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

    MMap<uint> predicate_index_;
    MMap<uint> predicate_index_arrays_no_compress_;
    MMap<uint8_t> predicate_index_arrays_;
    std::vector<std::shared_ptr<std::vector<uint>>> ps_sets_;
    std::vector<std::shared_ptr<std::vector<uint>>> po_sets_;

    std::vector<std::vector<uint>> subject_characteristic_set_;
    std::vector<std::vector<uint>> object_characteristic_set_;

    ulong max_subject_id_;

    ulong FileSize(std::string file_name);

    void Init();

    void LoadCharacteristicSet(std::vector<std::vector<uint>>& characteristic_sets, std::string filename);

    uint AccessBitSequence(MMap<uint>& bits, uint data_width, ulong bit_start);

    std::shared_ptr<std::vector<uint>> AccessAllArrays(DAA& daa, uint daa_start, uint daa_size);

    uint AccessToDAA(DAA& daa, ulong offset);

    uint AccessLevels(DAA& daa, ulong offset);

    std::shared_ptr<std::vector<uint>> AccessDAA(DAA& daa, uint offset, uint daa_start, uint daa_size);

   public:
    IndexRetriever();

    IndexRetriever(std::string db_name);

    void Close();

    const char* ID2String(uint id, SPARQLParser::Term::Positon pos);

    uint Term2ID(const SPARQLParser::Term& term);

    std::pair<uint, uint> FetchDAABounds(DAA& daa, uint id);

    std::shared_ptr<std::vector<uint>> GetSSet(uint pid);

    std::shared_ptr<std::vector<uint>> GetOSet(uint pid);

    std::shared_ptr<std::vector<uint>> GetSPreSet(uint sid);

    std::shared_ptr<std::vector<uint>> GetOPreSet(uint oid);

    std::shared_ptr<std::vector<uint>> GetByS(uint sid);

    std::shared_ptr<std::vector<uint>> GetByO(uint oid);

    std::shared_ptr<std::vector<uint>> GetBySP(uint sid, uint pid);

    std::shared_ptr<std::vector<uint>> GetByOP(uint oid, uint pid);

    std::shared_ptr<std::vector<uint>> GetBySO(uint sid, uint oid);

    uint GetSSetSize(uint pid);

    uint GetOSetSize(uint pid);

    uint GetBySSize(uint sid);

    uint GetByOSize(uint oid);

    uint GetBySPSize(uint sid, uint pid);

    uint GetByOPSize(uint oid, uint pid);

    uint GetBySOSize(uint sid, uint oid);

    uint predicate_cnt();

    uint shared_cnt();
};

#endif