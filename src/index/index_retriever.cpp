#include "rdf-tdaa/index/index_retriever.hpp"
#include "rdf-tdaa/utils/vbyte.hpp"

IndexRetriever::IndexRetriever() {}

IndexRetriever::IndexRetriever(std::string db_name) : db_name_(db_name) {
    auto beg = std::chrono::high_resolution_clock::now();

    db_dictionary_path_ = "./DB_DATA_ARCHIVE/" + db_name_ + "/dictionary/";
    db_index_path_ = "./DB_DATA_ARCHIVE/" + db_name_ + "/index/";

    dict_ = Dictionary(db_dictionary_path_);

    InitMMap();
    ps_sets_ = std::vector<std::span<uint>>(dict_.predicate_cnt());
    po_sets_ = std::vector<std::span<uint>>(dict_.predicate_cnt());

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

void IndexRetriever::InitMMap() {
    predicate_index_ = MMap<uint>(db_index_path_ + "predicate_index");

    std::string file_path = db_index_path_ + "predicate_index_arrays";
    if (predicate_index_compressed_)
        predicate_index_arrays_ = MMap<uint8_t>(file_path);
    else
        predicate_index_arrays_no_compress_ = MMap<uint>(file_path);

    spo_.to_daa = MMap<uint>(db_index_path_ + "spo_to_daa");
    spo_.daa_levels = MMap<uint>(db_index_path_ + "spo_daa_levels");
    spo_.daa_level_end = MMap<char>(db_index_path_ + "spo_daa_level_end");
    spo_.daa_array_end = MMap<char>(db_index_path_ + "spo_daa_array_end");

    ops_.to_daa = MMap<uint>(db_index_path_ + "ops_to_daa");
    ops_.daa_levels = MMap<uint>(db_index_path_ + "ops_daa_levels");
    ops_.daa_level_end = MMap<char>(db_index_path_ + "ops_daa_level_end");
    ops_.daa_array_end = MMap<char>(db_index_path_ + "ops_daa_array_end");

    MMap<uint> s_c_sets = MMap<uint>(db_index_path_ + "s_c_sets");
    uint count = s_c_sets[0];
    subject_characteristic_set_ = CharacteristicSet(count);
    subject_characteristic_set_.mmap = MMap<uint8_t>(db_index_path_ + "s_c_sets");
    for (uint set_id = 1; set_id <= count; set_id++)
        subject_characteristic_set_.offset_size[set_id - 1] = {s_c_sets[2 * set_id - 1],
                                                               s_c_sets[2 * set_id]};
    s_c_sets.CloseMap();

    MMap<uint> o_c_sets = MMap<uint>(db_index_path_ + "o_c_sets");
    count = o_c_sets[0];
    object_characteristic_set_ = CharacteristicSet(count);
    object_characteristic_set_.mmap = MMap<uint8_t>(db_index_path_ + "o_c_sets");
    for (uint set_id = 1; set_id <= count; set_id++)
        object_characteristic_set_.offset_size[set_id - 1] = {o_c_sets[2 * set_id - 1], o_c_sets[2 * set_id]};
    o_c_sets.CloseMap();

    if (spo_.to_daa_compressed_ || spo_.levels_compressed_) {
        MMap<uint> data_width = MMap<uint>(db_index_path_ + "data_width", 6 * 4);
        spo_.chara_set_id_width = data_width[0];
        spo_.daa_offset_width = data_width[1];
        spo_.daa_levels_width = data_width[2];
        ops_.chara_set_id_width = data_width[3];
        ops_.daa_offset_width = data_width[4];
        ops_.daa_levels_width = data_width[5];
        data_width.CloseMap();
    }
}

void IndexRetriever::Close() {
    predicate_index_.CloseMap();
    if (predicate_index_compressed_)
        predicate_index_arrays_.CloseMap();
    else
        predicate_index_arrays_no_compress_.CloseMap();

    spo_.to_daa.CloseMap();
    spo_.daa_levels.CloseMap();
    spo_.daa_level_end.CloseMap();
    spo_.daa_array_end.CloseMap();

    ops_.to_daa.CloseMap();
    ops_.daa_levels.CloseMap();
    ops_.daa_level_end.CloseMap();
    ops_.daa_array_end.CloseMap();

    std::vector<std::span<uint>>().swap(ps_sets_);
    std::vector<std::span<uint>>().swap(po_sets_);

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
    if (ps_sets_[pid - 1].size() == 0) {
        if (predicate_index_compressed_) {
            uint s_array_offset = predicate_index_[(pid - 1) * 4];
            uint s_array_size = predicate_index_[(pid - 1) * 4 + 2] - s_array_offset;

            uint8_t* compressed_buffer = new uint8_t[s_array_size];
            for (uint i = 0; i < s_array_size; i++)
                compressed_buffer[i] = predicate_index_arrays_[s_array_offset + i];

            uint total_length = predicate_index_[(pid - 1) * 4 + 1];
            uint* recovdata = new uint[total_length];

            streamvbyte_decode(compressed_buffer, recovdata, total_length);

            for (uint i = 1; i < total_length; i++)
                recovdata[i] += recovdata[i - 1];

            ps_sets_[pid - 1] = std::span<uint>(recovdata, total_length);
        } else {
            uint s_array_offset = predicate_index_[(pid - 1) * 2];
            uint s_array_size = predicate_index_[(pid - 1) * 2 + 1] - s_array_offset;

            uint* set = new uint[s_array_size];
            for (uint i = 0; i < s_array_size; i++)
                set[i] = predicate_index_arrays_no_compress_[s_array_offset + i];

            ps_sets_[pid - 1] = std::span<uint>(set, s_array_size);
        }
    }

    return ps_sets_[pid - 1];
}

// ?s p ?o
std::span<uint> IndexRetriever::GetOSet(uint pid) {
    if (po_sets_[pid - 1].size() == 0) {
        if (predicate_index_compressed_) {
            uint o_array_offset = predicate_index_[(pid - 1) * 4 + 2];
            uint o_array_size;
            if (pid != dict_.predicate_cnt())
                o_array_size = predicate_index_[pid * 4] - o_array_offset;
            else
                o_array_size = predicate_index_arrays_.size_ - o_array_offset;

            uint8_t* compressed_buffer = new uint8_t[o_array_size];
            for (uint i = 0; i < o_array_size; i++)
                compressed_buffer[i] = predicate_index_arrays_[o_array_offset + i];

            uint total_length = predicate_index_[(pid - 1) * 4 + 3];
            uint* recovdata = new uint[total_length];
            streamvbyte_decode(compressed_buffer, recovdata, total_length);

            for (uint i = 1; i < total_length; i++)
                recovdata[i] += recovdata[i - 1];

            po_sets_[pid - 1] = std::span<uint>(recovdata, total_length);
        } else {
            uint o_array_offset = predicate_index_[(pid - 1) * 2 + 1];
            uint o_array_size;
            if (pid != dict_.predicate_cnt())
                o_array_size = predicate_index_[pid * 2] - o_array_offset;
            else
                o_array_size = predicate_index_.size_ / 4 - o_array_offset;

            uint* set = new uint[o_array_size];
            for (uint i = 0; i < o_array_size; i++)
                set[i] = predicate_index_arrays_no_compress_[o_array_offset + i];

            po_sets_[pid - 1] = std::span<uint>(set, set + o_array_size);
        }
    }
    return po_sets_[pid - 1];
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
    if (ps_sets_[pid - 1].size() == 0) {
        uint s_array_size;
        if (predicate_index_compressed_) {
            s_array_size = predicate_index_[(pid - 1) * 4 + 1];
        } else {
            uint s_array_offset = predicate_index_[(pid - 1) * 2];
            s_array_size = predicate_index_[(pid - 1) * 2 + 1] - s_array_offset;
        }
        return s_array_size;
    }

    return ps_sets_[pid - 1].size();
}

uint IndexRetriever::GetOSetSize(uint pid) {
    if (po_sets_[pid - 1].size() == 0) {
        uint o_array_size;
        if (predicate_index_compressed_) {
            o_array_size = predicate_index_[(pid - 1) * 4 + 3];
        } else {
            uint o_array_offset = predicate_index_[(pid - 1) * 2 + 1];
            if (pid != dict_.predicate_cnt())
                o_array_size = predicate_index_[pid * 2] - o_array_offset;
            else
                o_array_size = predicate_index_.size_ / 4 - o_array_offset;
        }
        return o_array_size;
    }
    return po_sets_[pid - 1].size();
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