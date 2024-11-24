#include "rdf-tdaa/index/index_retriever.hpp"
#include "rdf-tdaa/index/predicate_index.hpp"
#include "rdf-tdaa/utils/join_list.hpp"
#include "rdf-tdaa/utils/vbyte.hpp"
#include "streamvbyte.h"

IndexRetriever::IndexRetriever() {}

IndexRetriever::IndexRetriever(std::string db_name) : db_path_(db_name) {
    db_dictionary_path_ = db_path_ + "/dictionary/";
    db_index_path_ = db_path_ + "/index/";

    auto beg = std::chrono::high_resolution_clock::now();
    dict_ = Dictionary(db_dictionary_path_);
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> diff = end - beg;
    std::cout << "init string dictionary takes " << diff.count() << " ms." << std::endl;

    max_subject_id_ = dict_.shared_cnt() + dict_.subject_cnt();

    predicate_index_ = PredicateIndex(db_index_path_, dict_.predicate_cnt());

    spo_ = DAAs(db_index_path_, DAAs::Type::kSPO, predicate_index_);
    spo_.Load();
    ops_ = DAAs(db_index_path_, DAAs::Type::kOPS, predicate_index_);
    ops_.Load();
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
        return spo_.CharacteristicSetOf(sid);
    }
    return std::span<uint>();
}

// ?s ?p o, s ?p ?o
std::span<uint> IndexRetriever::GetOPreSet(uint oid) {
    if ((0 < oid && oid <= dict_.shared_cnt()) || max_subject_id_ < oid) {
        if (oid > dict_.shared_cnt())
            oid -= dict_.subject_cnt();
        return ops_.CharacteristicSetOf(oid);
    }
    return std::span<uint>();
}

// s p ?o
std::span<uint> IndexRetriever::GetBySP(uint sid, uint pid) {
    if (0 < sid && sid <= max_subject_id_) {
        const auto& char_set = spo_.CharacteristicSetOf(sid);
        auto it = std::lower_bound(char_set.begin(), char_set.end(), pid);

        if (it != char_set.end() && *it == pid) {
            uint index = std::distance(char_set.begin(), it);
            return spo_.AccessDAA(sid, pid, index);
        }
    }
    return std::span<uint>();
}

// ?s p o
std::span<uint> IndexRetriever::GetByOP(uint oid, uint pid) {
    if ((0 < oid && oid <= dict_.shared_cnt()) || max_subject_id_ < oid) {
        if (oid > dict_.shared_cnt())
            oid -= dict_.subject_cnt();

        const auto& char_set = ops_.CharacteristicSetOf(oid);
        auto it = std::lower_bound(char_set.begin(), char_set.end(), pid);

        if (it != char_set.end() && *it == pid) {
            uint index = std::distance(char_set.begin(), it);
            return ops_.AccessDAA(oid, pid, index);
        }
    }
    return std::span<uint>();
}

// s ?p o
std::span<uint> IndexRetriever::GetBySO(uint sid, uint oid) {
    std::vector<uint>* result = new std::vector<uint>;

    if ((0 < sid && sid <= max_subject_id_) && (oid <= dict_.shared_cnt() || max_subject_id_ < oid)) {
        uint original_oid = oid;
        if (oid > dict_.shared_cnt())
            oid -= dict_.subject_cnt();

        std::span<uint>& s_c_set = spo_.CharacteristicSetOf(sid);
        std::span<uint>& o_c_set = ops_.CharacteristicSetOf(oid);

        for (uint i = 0; i < s_c_set.size(); i++) {
            for (uint j = 0; j < o_c_set.size(); j++) {
                if (s_c_set[i] == o_c_set[j]) {
                    auto r = GetBySP(sid, s_c_set[i]);
                    bool found = std::binary_search(r.begin(), r.end(), original_oid);
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