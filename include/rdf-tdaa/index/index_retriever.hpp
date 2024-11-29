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
#include "rdf-tdaa/index/cs_daa_map.hpp"
#include "rdf-tdaa/index/daas.hpp"
#include "rdf-tdaa/index/predicate_index.hpp"

class IndexRetriever {
    std::string db_path_;
    std::string db_dictionary_path_;
    std::string db_index_path_;
    std::string spo_index_path_;
    std::string ops_index_path_;

    Dictionary dict_;

    CharacteristicSet subject_characteristic_set_;
    CharacteristicSet object_characteristic_set_;
    CsDaaMap cs_daa_map_;
    DAAs spo_;
    DAAs ops_;

    PredicateIndex predicate_index_;

    ulong max_subject_id_;

    ulong FileSize(std::string file_name);

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