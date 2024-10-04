#include "rdf-tdaa/index/index_retriever.hpp"
#include "rdf-tdaa/index/predicate_index.hpp"
#include "rdf-tdaa/utils/join_list.hpp"
#include "rdf-tdaa/utils/vbyte.hpp"
#include "streamvbyte.h"

IndexRetriever::IndexRetriever() {}

IndexRetriever::IndexRetriever(std::string db_name) : db_name_(db_name) {
    auto beg = std::chrono::high_resolution_clock::now();

    db_dictionary_path_ = "./DB_DATA_ARCHIVE/" + db_name_ + "/dictionary/";
    db_index_path_ = "./DB_DATA_ARCHIVE/" + db_name_ + "/index/";

    dict_ = Dictionary(db_dictionary_path_);

    spo_ = DAAs(db_index_path_, DAAs::Type::kSPO);
    spo_.Load();
    ops_ = DAAs(db_index_path_, DAAs::Type::kOPS);
    ops_.Load();

    subject_characteristic_set_ = CharacteristicSet(db_index_path_ + "s_c_sets");
    subject_characteristic_set_.Load();
    object_characteristic_set_ = CharacteristicSet(db_index_path_ + "o_c_sets");
    object_characteristic_set_.Load();

    predicate_index_ = PredicateIndex(db_index_path_, dict_.predicate_cnt());

    max_subject_id_ = dict_.shared_cnt() + dict_.subject_cnt();

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> diff = end - beg;
    std::cout << "init db success. takes " << diff.count() << " ms." << std::endl;
}

ulong IndexRetriever::FileSize(std::string file_name) {
    std::ifstream file(file_name, std::ios::binary | std::ios::ate);
    ulong size = static_cast<ulong>(file.tellg());
    file.close();
    return size;
}

void IndexRetriever::Close() {
    predicate_index_.Close();
    spo_.Close();
    ops_.Close();
    dict_.Close();
}

const char* IndexRetriever::ID2String(uint id, SPARQLParser::Term::Positon pos) {
    return dict_.ID2String(id, pos);
}

uint IndexRetriever::Term2ID(const SPARQLParser::Term& term) {
    return dict_.String2ID(term.value, term.position);
}

// ?s p ?o
std::span<uint> IndexRetriever::GetSSet(uint pid) {
    return predicate_index_.GetSSet(pid);
}

// ?s p ?o
std::span<uint> IndexRetriever::GetOSet(uint pid) {
    return predicate_index_.GetOSet(pid);
}

// ?s ?p o, s ?p ?o
std::span<uint> IndexRetriever::GetSPreSet(uint sid) {
    if (0 < sid && sid <= max_subject_id_) {
        uint c_set_id = spo_.CharacteristicSetID(sid);
        return subject_characteristic_set_[c_set_id];
    }
    return std::span<uint>();
}

// ?s ?p o, s ?p ?o
std::span<uint> IndexRetriever::GetOPreSet(uint oid) {
    if ((0 < oid && oid <= dict_.shared_cnt()) || max_subject_id_ < oid) {
        if (oid > dict_.shared_cnt())
            oid -= dict_.subject_cnt();
        uint c_set_id = ops_.CharacteristicSetID(oid);
        return object_characteristic_set_[c_set_id];
    }
    return std::span<uint>();
}

// s p ?o
std::span<uint> IndexRetriever::GetBySP(uint sid, uint pid) {
    if (0 < sid && sid <= max_subject_id_) {
        uint c_set_id = spo_.CharacteristicSetID(sid);
        const auto& char_set = subject_characteristic_set_[c_set_id];
        auto it = std::lower_bound(char_set.begin(), char_set.end(), pid);

        if (it != char_set.end() && *it == pid) {
            uint index = std::distance(char_set.begin(), it);
            return spo_.AccessDAA(sid, index);
        }
    }
    return std::span<uint>();
}

// ?s p o
std::span<uint> IndexRetriever::GetByOP(uint oid, uint pid) {
    if ((0 < oid && oid <= dict_.shared_cnt()) || max_subject_id_ < oid) {
        if (oid > dict_.shared_cnt())
            oid -= dict_.subject_cnt();

        uint c_set_id = ops_.CharacteristicSetID(oid);
        const auto& char_set = object_characteristic_set_[c_set_id];
        auto it = std::lower_bound(char_set.begin(), char_set.end(), pid);

        if (it != char_set.end() && *it == pid) {
            uint index = std::distance(char_set.begin(), it);
            return ops_.AccessDAA(oid, index);
        }
    }
    return std::span<uint>();
}

// s ?p o
std::span<uint> IndexRetriever::GetBySO(uint sid, uint oid) {
    std::vector<uint>* result = new std::vector<uint>;

    if ((0 < sid && sid <= max_subject_id_) && (oid <= dict_.shared_cnt() || max_subject_id_ < oid)) {
        if (oid > dict_.shared_cnt())
            oid -= dict_.subject_cnt();

        uint s_c_set_id = spo_.CharacteristicSetID(sid);
        uint o_c_set_id = ops_.CharacteristicSetID(oid);
        std::span<uint>& s_c_set = subject_characteristic_set_[s_c_set_id];
        std::span<uint>& o_c_set = object_characteristic_set_[o_c_set_id];

        for (uint i = 0; i < s_c_set.size(); i++) {
            for (uint j = 0; j < o_c_set.size(); j++) {
                if (s_c_set[i] == o_c_set[j]) {
                    auto r = GetBySP(sid, s_c_set[i]);
                    bool found = std::binary_search(r.begin(), r.end(), oid);
                    if (found)
                        result->push_back(s_c_set[i]);
                }
            }
        }
    }

    return std::span<uint>(*result);
}

std::span<uint> IndexRetriever::GetByS(uint sid) {
    if (0 < sid && sid <= max_subject_id_) {
        std::span<uint> result = spo_.AccessDAAAllArrays(sid);
        std::sort(result.begin(), result.end());
        auto it = std::unique(result.begin(), result.end());
        return std::span<uint>(result.begin(), std::distance(result.data(), &*it));
    }
    return std::span<uint>();
}

std::span<uint> IndexRetriever::GetByO(uint oid) {
    if ((0 < oid && oid <= dict_.shared_cnt()) || max_subject_id_ < oid) {
        if (oid > dict_.shared_cnt())
            oid -= dict_.subject_cnt();

        std::span<uint> result = ops_.AccessDAAAllArrays(oid);
        std::sort(result.begin(), result.end());
        auto it = std::unique(result.begin(), result.end());
        return std::span<uint>(result.begin(), std::distance(result.data(), &*it));
    }
    return std::span<uint>();
}

uint IndexRetriever::GetSSetSize(uint pid) {
    return predicate_index_.GetSSetSize(pid);
}

uint IndexRetriever::GetOSetSize(uint pid) {
    return predicate_index_.GetOSetSize(pid);
}

uint IndexRetriever::GetBySSize(uint sid) {
    if (sid <= max_subject_id_)
        return spo_.DAASize(sid);
    return 0;
}

uint IndexRetriever::GetByOSize(uint oid) {
    if ((0 < oid && oid <= dict_.shared_cnt()) || max_subject_id_ < oid)
        return ops_.DAASize(oid);
    return 0;
}

uint IndexRetriever::GetBySPSize(uint sid, uint pid) {
    return GetBySP(sid, pid).size();
}

uint IndexRetriever::GetByOPSize(uint oid, uint pid) {
    return GetByOP(oid, pid).size();
}

uint IndexRetriever::GetBySOSize(uint sid, uint oid) {
    return GetBySO(sid, oid).size();
}

uint IndexRetriever::predicate_cnt() {
    return dict_.predicate_cnt();
}

uint IndexRetriever::shared_cnt() {
    return dict_.shared_cnt();
}