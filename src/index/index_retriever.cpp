#include "rdf-tdaa/index/index_retriever.hpp"
#include "rdf-tdaa/index/predicate_index.hpp"
#include "rdf-tdaa/utils/join_list.hpp"
#include "rdf-tdaa/utils/vbyte.hpp"
#include "streamvbyte.h"

IndexRetriever::IndexRetriever() {}

IndexRetriever::IndexRetriever(std::string db_name) : db_path_(db_name) {
    db_dictionary_path_ = db_path_ + "/dictionary/";
    db_index_path_ = db_path_ + "/index/";
    spo_index_path_ = db_index_path_ + "spo/";
    ops_index_path_ = db_index_path_ + "ops/";

    auto beg = std::chrono::high_resolution_clock::now();
    dict_ = Dictionary(db_dictionary_path_);
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> diff = end - beg;

    max_subject_id_ = dict_.shared_cnt() + dict_.subject_cnt();

    predicate_index_ = PredicateIndex(db_index_path_, dict_.predicate_cnt());

    std::pair<uint, uint> cs_id_width;
    std::pair<uint, uint> daa_offset_width;

    MMap<uint> metadata = MMap<uint>(db_index_path_ + "metadata");
    uint shared_id_size = metadata[0];
    cs_id_width.first = metadata[1];
    cs_id_width.second = metadata[2];
    daa_offset_width.first = metadata[3];
    daa_offset_width.second = metadata[4];
    uint not_shared_cs_id_width = metadata[5];
    uint not_shared_daa_offset_width = metadata[6];
    uint spo_daa_levels_width = metadata[7];
    uint ops_daa_levels_width = metadata[8];
    metadata.CloseMap();

    cs_daa_map_ = CsDaaMap(db_index_path_ + "cs_daa_map", cs_id_width, daa_offset_width,
                           not_shared_cs_id_width, not_shared_daa_offset_width, dict_.shared_cnt(),
                           dict_.subject_cnt(), dict_.object_cnt(), shared_id_size);

    spo_ = DAAs(spo_index_path_, spo_daa_levels_width);
    spo_.Load();
    ops_ = DAAs(ops_index_path_, ops_daa_levels_width);
    ops_.Load();

    subject_characteristic_set_ = CharacteristicSet(db_index_path_ + "s_c_sets");
    subject_characteristic_set_.Load();
    object_characteristic_set_ = CharacteristicSet(db_index_path_ + "o_c_sets");
    object_characteristic_set_.Load();

    std::cout << "init string dictionary takes " << diff.count() << " ms." << std::endl;
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
        uint cs_id = cs_daa_map_.ChararisticSetIdOf(sid, CsDaaMap::Permutation::kSPO);
        return subject_characteristic_set_[cs_id];
    }
    return std::span<uint>();
}

// ?s ?p o, s ?p ?o
std::span<uint> IndexRetriever::GetOPreSet(uint oid) {
    if ((0 < oid && oid <= dict_.shared_cnt()) || max_subject_id_ < oid) {
        uint cs_id = cs_daa_map_.ChararisticSetIdOf(oid, CsDaaMap::Permutation::kOPS);
        return object_characteristic_set_[cs_id];
    }
    return std::span<uint>();
}

// s p ?o
std::span<uint> IndexRetriever::GetBySP(uint sid, uint pid) {
    if (0 < sid && sid <= max_subject_id_) {
        uint cs_id = cs_daa_map_.ChararisticSetIdOf(sid, CsDaaMap::Permutation::kSPO);
        const auto& char_set = subject_characteristic_set_[cs_id];
        auto it = std::lower_bound(char_set.begin(), char_set.end(), pid);

        if (it != char_set.end() && *it == pid) {
            uint index = std::distance(char_set.begin(), it);
            auto [offset, size] = cs_daa_map_.DAAOffsetSizeOf(sid, CsDaaMap::Permutation::kSPO);
            return spo_.AccessDAA(offset, size, predicate_index_.GetOSet(pid), index);
        }
    }
    return std::span<uint>();
}

// ?s p o
std::span<uint> IndexRetriever::GetByOP(uint oid, uint pid) {
    if ((0 < oid && oid <= dict_.shared_cnt()) || max_subject_id_ < oid) {
        uint cs_id = cs_daa_map_.ChararisticSetIdOf(oid, CsDaaMap::Permutation::kOPS);
        const auto& char_set = object_characteristic_set_[cs_id];
        auto it = std::lower_bound(char_set.begin(), char_set.end(), pid);

        if (it != char_set.end() && *it == pid) {
            uint index = std::distance(char_set.begin(), it);
            auto [offset, size] = cs_daa_map_.DAAOffsetSizeOf(oid, CsDaaMap::Permutation::kOPS);
            return ops_.AccessDAA(offset, size, predicate_index_.GetSSet(pid), index);
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

        uint cs_id = cs_daa_map_.ChararisticSetIdOf(sid, CsDaaMap::Permutation::kSPO);
        std::span<uint>& s_c_set = subject_characteristic_set_[cs_id];
        cs_id = cs_daa_map_.ChararisticSetIdOf(oid, CsDaaMap::Permutation::kOPS);
        std::span<uint>& o_c_set = object_characteristic_set_[cs_id];

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
        uint cs_id = cs_daa_map_.ChararisticSetIdOf(sid, CsDaaMap::Permutation::kSPO);
        const auto& char_set = subject_characteristic_set_[cs_id];
        std::vector<std::span<uint>> offset2id;
        for (auto& pid : char_set)
            offset2id.push_back(predicate_index_.GetOSet(pid));

        auto [offset, size] = cs_daa_map_.DAAOffsetSizeOf(sid, CsDaaMap::Permutation::kSPO);
        std::span<uint> result = spo_.AccessDAAAllArrays(offset, size, offset2id);
        std::sort(result.begin(), result.end());
        auto it = std::unique(result.begin(), result.end());
        return std::span<uint>(result.begin(), std::distance(result.data(), &*it));
    }
    return std::span<uint>();
}

std::span<uint> IndexRetriever::GetByO(uint oid) {
    if ((0 < oid && oid <= dict_.shared_cnt()) || max_subject_id_ < oid) {
        uint cs_id = cs_daa_map_.ChararisticSetIdOf(oid, CsDaaMap::Permutation::kOPS);
        const auto& char_set = object_characteristic_set_[cs_id];
        std::vector<std::span<uint>> offset2id;
        for (auto& pid : char_set)
            offset2id.push_back(predicate_index_.GetSSet(pid));

        auto [offset, size] = cs_daa_map_.DAAOffsetSizeOf(oid, CsDaaMap::Permutation::kOPS);
        std::span<uint> result = ops_.AccessDAAAllArrays(offset, size, offset2id);
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
        return cs_daa_map_.DAAOffsetSizeOf(sid, CsDaaMap::Permutation::kSPO).second;
    return 0;
}

uint IndexRetriever::GetByOSize(uint oid) {
    if ((0 < oid && oid <= dict_.shared_cnt()) || max_subject_id_ < oid)
        return cs_daa_map_.DAAOffsetSizeOf(oid, CsDaaMap::Permutation::kOPS).second;
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