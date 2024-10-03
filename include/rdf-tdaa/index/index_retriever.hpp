#ifndef INDEX_RETRIEVER_HPP
#define INDEX_RETRIEVER_HPP

#include <limits.h>
#include <fstream>
#include <iostream>
#include <span>
#include <thread>
#include <vector>
#include "rdf-tdaa/dictionary/dictionary.hpp"
#include "rdf-tdaa/index/characteristic_set.hpp"
#include "rdf-tdaa/index/daas.hpp"
#include "rdf-tdaa/utils/join_list.hpp"
#include "rdf-tdaa/utils/mmap.hpp"
#include "streamvbyte.h"

class IndexRetriever {
    std::string db_name_;
    std::string db_dictionary_path_;
    std::string db_index_path_;

    bool predicate_index_compressed_ = true;

    Dictionary dict_;

    DAAs spo_;
    DAAs ops_;

    MMap<uint> predicate_index_;
    MMap<uint> predicate_index_arrays_no_compress_;
    MMap<uint8_t> predicate_index_arrays_;
    std::vector<std::span<uint>> ps_sets_;
    std::vector<std::span<uint>> po_sets_;

    CharacteristicSet subject_characteristic_set_;
    CharacteristicSet object_characteristic_set_;

    ulong max_subject_id_;

    ulong FileSize(std::string file_name);

    void InitMMap();

   public:
    IndexRetriever();

    IndexRetriever(std::string db_name);

    void Close();

    const char* ID2String(uint id, SPARQLParser::Term::Positon pos);

    uint Term2ID(const SPARQLParser::Term& term);

    std::span<uint> GetSSet(uint pid);

    std::span<uint> GetOSet(uint pid);

    std::span<uint> GetSPreSet(uint sid);

    std::span<uint> GetOPreSet(uint oid);

    std::span<uint> GetByS(uint sid);

    std::span<uint> GetByO(uint oid);

    std::span<uint> GetBySP(uint sid, uint pid);

    std::span<uint> GetByOP(uint oid, uint pid);

    std::span<uint> GetBySO(uint sid, uint oid);

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