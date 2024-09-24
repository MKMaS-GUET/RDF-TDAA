#include "rdf-tdaa/index/index_retriever.hpp"
#include "rdf-tdaa/utils/vbyte.hpp"

One::One(MMap<char>& bits, uint begin, uint end) : bits_(bits), bit_offset_(begin), end_(end) {}

void printBits(uint num, uint width) {
    for (int i = width - 1; i >= 0; --i) {
        uint bit = (num >> i) & 1;
        std::cout << bit;
    }
}

// next one in [begin, end)
uint One::Next() {
    for (; bit_offset_ < end_; bit_offset_++) {
        if (bits_[bit_offset_ / 8] != 0) {
            if (bits_[bit_offset_ / 8] & 1 << (7 - bit_offset_ % 8)) {
                bit_offset_++;
                return bit_offset_ - 1;
            }
        } else {
            bit_offset_ = bit_offset_ - bit_offset_ % 8 + 7;
        }
    }
    return end_;
}

// ones in [begin, end)
uint range_rank(MMap<char>& bits, uint begin, uint end) {
    uint cnt = 0;
    for (uint bit_offset = begin; bit_offset < end; bit_offset++) {
        if (bits[bit_offset / 8] != 0) {
            if (bits[bit_offset / 8] & 1 << (7 - bit_offset % 8))
                cnt++;
        } else {
            bit_offset = bit_offset - bit_offset % 8 + 7;
        }
    }
    return cnt;
}

IndexRetriever::IndexRetriever() {}

IndexRetriever::IndexRetriever(std::string db_name) : db_name_(db_name) {
    auto beg = std::chrono::high_resolution_clock::now();

    db_dictionary_path_ = "./DB_DATA_ARCHIVE/" + db_name_ + "/dictionary/";
    db_index_path_ = "./DB_DATA_ARCHIVE/" + db_name_ + "/index/";

    dict_ = Dictionary(db_dictionary_path_);

    auto build_cache = [&](uint start, uint end) {
        for (uint id = start; id <= end; id++) {
            if (id <= shared_cnt()) {
                if (id % 10 == 0)
                    delete[] dict_.ID2String(id, SPARQLParser::Term::Positon::kShared);
            } else if (id <= dict_.shared_cnt() + dict_.subject_cnt()) {
                if (id % 10 == 0)
                    delete[] dict_.ID2String(id, SPARQLParser::Term::Positon::kSubject);
            } else {
                if (id % 10 == 0)
                    delete[] dict_.ID2String(id, SPARQLParser::Term::Positon::kObject);
            }
        }
    };

    uint cpu_count = std::thread::hardware_concurrency();
    uint cnt = dict_.shared_cnt() + dict_.subject_cnt() + dict_.object_cnt();
    uint batch_size = cnt / cpu_count;

    std::vector<std::thread> threads;
    for (uint i = 0; i < cpu_count; i++) {
        uint start = i * batch_size + 1;
        uint end = (i + 1) * batch_size;
        threads.emplace_back(std::thread([start, end, &build_cache]() { build_cache(start, end); }));
    }
    for (auto& t : threads)
        t.join();

    Init();
    ps_sets_ = std::vector<std::shared_ptr<std::vector<uint>>>(dict_.predicate_cnt());
    for (auto& set : ps_sets_)
        set = std::make_shared<std::vector<uint>>();
    po_sets_ = std::vector<std::shared_ptr<std::vector<uint>>>(dict_.predicate_cnt());
    for (auto& set : po_sets_)
        set = std::make_shared<std::vector<uint>>();

    LoadCharacteristicSet(subject_characteristic_set_, db_index_path_ + "s_c_sets");
    LoadCharacteristicSet(object_characteristic_set_, db_index_path_ + "o_c_sets");

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

void IndexRetriever::Init() {
    predicate_index_ = MMap<uint>(db_index_path_ + "predicate_index");

    std::string file_path = db_index_path_ + "predicate_index_arrays";
    if (predicate_index_compressed_)
        predicate_index_arrays_ = MMap<uint8_t>(file_path);
    else
        predicate_index_arrays_no_compress_ = MMap<uint>(file_path);

    spo_.to_daa_ = MMap<uint>(db_index_path_ + "spo_to_daa");
    spo_.daa_levels_ = MMap<uint>(db_index_path_ + "spo_daa_levels");
    spo_.daa_level_end_ = MMap<char>(db_index_path_ + "spo_daa_level_end");
    spo_.daa_array_end_ = MMap<char>(db_index_path_ + "spo_daa_array_end");

    ops_.to_daa_ = MMap<uint>(db_index_path_ + "ops_to_daa");
    ops_.daa_levels_ = MMap<uint>(db_index_path_ + "ops_daa_levels");
    ops_.daa_level_end_ = MMap<char>(db_index_path_ + "ops_daa_level_end");
    ops_.daa_array_end_ = MMap<char>(db_index_path_ + "ops_daa_array_end");

    if (to_daa_compressed_ || levels_compressed_) {
        MMap<uint> data_width = MMap<uint>(db_index_path_ + "data_width", 6 * 4);
        spo_.chara_set_id_width_ = data_width[0];
        spo_.daa_offset_width_ = data_width[1];
        spo_.daa_levels_width_ = data_width[2];
        ops_.chara_set_id_width_ = data_width[3];
        ops_.daa_offset_width_ = data_width[4];
        ops_.daa_levels_width_ = data_width[5];
        data_width.CloseMap();
    }
}

void IndexRetriever::LoadCharacteristicSet(std::vector<std::vector<uint>>& characteristic_sets,
                                           std::string filename) {
    // 读取压缩后的数据长度
    auto [recovdata, total_length] = LoadAndDecompress(filename);

    std::vector<uint> p_set;
    uint value;
    for (uint offset = 0; offset < total_length; offset++) {
        value = recovdata[offset];
        if (value != 0) {
            p_set.push_back(recovdata[offset]);
        } else {
            p_set.shrink_to_fit();
            characteristic_sets.push_back(p_set);
            p_set.clear();
        }
    }
    characteristic_sets.shrink_to_fit();

    delete[] recovdata;
}

uint IndexRetriever::AccessBitSequence(MMap<uint>& bits, uint data_width, ulong bit_start) {
    uint uint_base = bit_start / 32;
    uint offset_in_uint = bit_start % 32;

    uint uint_cnt = (offset_in_uint + data_width + 31) / 32;
    uint data = 0;
    uint remaining_bits = data_width;

    for (uint uint_offset = uint_base; uint_offset < uint_base + uint_cnt; uint_offset++) {
        // 计算可以写入的位数
        uint bits_to_write = std::min(32 - offset_in_uint, remaining_bits);

        // 生成掩码
        uint shift_to_end = 32 - (offset_in_uint + bits_to_write);
        uint mask = ((1ull << bits_to_write) - 1) << shift_to_end;

        // 提取所需位并移位到目标位置
        uint extracted_bits = (bits[uint_offset] & mask) >> shift_to_end;
        data |= extracted_bits << (remaining_bits - bits_to_write);

        remaining_bits -= bits_to_write;
        offset_in_uint = 0;
    }

    return data;
}

std::shared_ptr<std::vector<uint>> IndexRetriever::AccessAllArrays(DAA& daa, uint daa_offset, uint daa_size) {
    MMap<uint>& levels = daa.daa_levels_;
    uint data_width = daa.daa_levels_width_;
    ulong bit_start = daa_offset * ulong(data_width);

    uint uint_base = bit_start / 32;
    uint offset_in_uint = bit_start % 32;

    uint uint_cnt = (offset_in_uint + daa_size * data_width + 31) / 32;
    uint data = 0;
    uint remaining_bits = data_width;

    std::vector<uint> levels_mem = std::vector<uint>();

    for (uint uint_offset = uint_base; uint_offset < uint_base + uint_cnt; uint_offset++) {
        uint bits_to_write = std::min(32 - offset_in_uint, remaining_bits);
        uint shift_to_end = 32 - (offset_in_uint + bits_to_write);
        uint mask = ((1ull << bits_to_write) - 1) << shift_to_end;

        uint extracted_bits = (levels[uint_offset] & mask) >> shift_to_end;
        data |= extracted_bits << (remaining_bits - bits_to_write);

        remaining_bits -= bits_to_write;
        offset_in_uint += bits_to_write;
        if (remaining_bits == 0) {
            levels_mem.push_back(data);

            remaining_bits = data_width;
            data = 0;
            if (offset_in_uint != 32)
                uint_offset--;
        }
        if (offset_in_uint == 32)
            offset_in_uint = 0;
    }

    std::shared_ptr<std::vector<std::vector<uint>>> arrays;

    if (daa_size == 1)
        return std::make_shared<std::vector<uint>>(std::vector<uint>{levels_mem[0]});

    std::vector<uint> level_starts;
    One one = One(daa.daa_level_end_, daa_offset, daa_offset + daa_size);
    uint end = one.Next();
    while (end != daa_offset + daa_size) {
        level_starts.push_back(end + 1);
        end = one.Next();
    }
    std::shared_ptr<std::vector<uint>> result = std::make_shared<std::vector<uint>>();

    MMap<char>& array_end = daa.daa_array_end_;

    uint predicate_cnt = level_starts[0] - daa_offset;
    for (uint p = 0; p < predicate_cnt; p++) {
        uint offset = p;
        uint value_cnt = 0;

        uint levels_offset = daa_offset + offset;
        result->push_back(levels_mem[levels_offset - daa_offset]);
        uint level_start = daa_offset;
        while (!get_bit(array_end, levels_offset)) {
            offset = offset - range_rank(array_end, level_start, level_start + offset);

            level_start = level_starts[value_cnt];
            levels_offset = level_start + offset;
            value_cnt++;

            result->push_back(levels_mem[levels_offset - daa_offset] + result->back());
        }
    }

    return result;
}

uint IndexRetriever::AccessToDAA(DAA& daa, ulong offset) {
    if (to_daa_compressed_) {
        uint data_width = (offset % 2) ? daa.daa_offset_width_ : daa.chara_set_id_width_;

        ulong bit_start = ulong(offset / 2) * ulong(daa.chara_set_id_width_ + daa.daa_offset_width_) +
                          ((offset % 2) ? daa.chara_set_id_width_ : 0);

        return AccessBitSequence(daa.to_daa_, data_width, bit_start);
    } else {
        return daa.to_daa_[offset];
    }
}

uint IndexRetriever::AccessLevels(DAA& daa, ulong offset) {
    MMap<uint>& levels = daa.daa_levels_;
    if (levels_compressed_) {
        uint data_width = daa.daa_levels_width_;
        ulong bit_start = offset * ulong(data_width);
        return AccessBitSequence(levels, data_width, bit_start);
    } else {
        return levels[offset];
    }
}

std::shared_ptr<std::vector<uint>> IndexRetriever::AccessDAA(DAA& daa,
                                                             uint offset,
                                                             uint daa_offset,
                                                             uint daa_size) {
    uint value;
    uint value_offset;
    MMap<char>& level_end = daa.daa_level_end_;
    MMap<char>& array_end = daa.daa_array_end_;

    value_offset = daa_offset + offset;
    // std::cout << offset << std::endl;
    value = AccessLevels(daa, value_offset);

    if (daa_size == 1)
        return std::make_shared<std::vector<uint>>(std::vector<uint>{value});

    std::shared_ptr<std::vector<uint>> array = std::make_shared<std::vector<uint>>();
    array->push_back(value);

    One one = One(level_end, daa_offset, daa_offset + daa_size);

    uint level_start = daa_offset;
    while (!get_bit(array_end, value_offset)) {
        offset = offset - range_rank(array_end, level_start, level_start + offset);

        level_start = one.Next() + 1;
        value_offset = level_start + offset;

        value = AccessLevels(daa, value_offset) + array->back();
        array->push_back(value);
    }

    return array;
}

void IndexRetriever::Close() {
    predicate_index_.CloseMap();
    if (predicate_index_compressed_)
        predicate_index_arrays_.CloseMap();
    else
        predicate_index_arrays_no_compress_.CloseMap();

    spo_.to_daa_.CloseMap();
    spo_.daa_levels_.CloseMap();
    spo_.daa_level_end_.CloseMap();
    spo_.daa_array_end_.CloseMap();

    ops_.to_daa_.CloseMap();
    ops_.daa_levels_.CloseMap();
    ops_.daa_level_end_.CloseMap();
    ops_.daa_array_end_.CloseMap();

    std::vector<std::shared_ptr<std::vector<uint>>>().swap(ps_sets_);
    std::vector<std::shared_ptr<std::vector<uint>>>().swap(po_sets_);

    dict_.Close();
}

const char* IndexRetriever::ID2String(uint id, SPARQLParser::Term::Positon pos) {
    return dict_.ID2String(id, pos);
}

uint IndexRetriever::Term2ID(const SPARQLParser::Term& term) {
    return dict_.String2ID(term.value_, term.position_);
}

std::pair<uint, uint> IndexRetriever::FetchDAABounds(DAA& daa, uint id) {
    uint daa_offset = 0;
    uint daa_size = AccessToDAA(daa, (id - 1) * 2 + 1);  // where next daa start
    if (id != 1) {
        daa_offset = AccessToDAA(daa, (id - 2) * 2 + 1);
        daa_size = daa_size - daa_offset;
    }
    return {daa_offset, daa_size};
}

// ?s p ?o
std::shared_ptr<std::vector<uint>> IndexRetriever::GetSSet(uint pid) {
    if (ps_sets_[pid - 1]->size() == 0) {
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

            ps_sets_[pid - 1] = std::make_shared<std::vector<uint>>(recovdata, recovdata + total_length);
        } else {
            uint s_array_offset = predicate_index_[(pid - 1) * 2];
            uint s_array_size = predicate_index_[(pid - 1) * 2 + 1] - s_array_offset;

            uint* set = new uint[s_array_size];
            for (uint i = 0; i < s_array_size; i++)
                set[i] = predicate_index_arrays_no_compress_[s_array_offset + i];

            ps_sets_[pid - 1] = std::make_shared<std::vector<uint>>(set, set + s_array_size);
        }
    }
    return ps_sets_[pid - 1];
}

// ?s p ?o
std::shared_ptr<std::vector<uint>> IndexRetriever::GetOSet(uint pid) {
    if (po_sets_[pid - 1]->size() == 0) {
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

            po_sets_[pid - 1] = std::make_shared<std::vector<uint>>(recovdata, recovdata + total_length);
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

            po_sets_[pid - 1] = std::make_shared<std::vector<uint>>(set, set + o_array_size);
        }
    }
    return po_sets_[pid - 1];
}

// ?s ?p o, s ?p ?o
std::shared_ptr<std::vector<uint>> IndexRetriever::GetSPreSet(uint sid) {
    if (0 < sid && sid <= max_subject_id_) {
        uint c_set_id = AccessToDAA(spo_, (sid - 1) * 2);
        return std::make_shared<std::vector<uint>>(subject_characteristic_set_[c_set_id - 1]);
    }
    return std::make_shared<std::vector<uint>>();
}

// ?s ?p o, s ?p ?o
std::shared_ptr<std::vector<uint>> IndexRetriever::GetOPreSet(uint oid) {
    if ((0 < oid && oid <= dict_.shared_cnt()) || max_subject_id_ < oid) {
        if (oid > dict_.shared_cnt())
            oid -= dict_.subject_cnt();
        uint c_set_id = AccessToDAA(ops_, (oid - 1) * 2);
        return std::make_shared<std::vector<uint>>(object_characteristic_set_[c_set_id - 1]);
    }
    return std::make_shared<std::vector<uint>>();
}

// s p ?o
std::shared_ptr<std::vector<uint>> IndexRetriever::GetBySP(uint sid, uint pid) {
    if (0 < sid && sid <= max_subject_id_) {
        uint c_set_id = AccessToDAA(spo_, (sid - 1) * 2);
        for (uint i = 0; i < subject_characteristic_set_[c_set_id - 1].size(); i++) {
            if (subject_characteristic_set_[c_set_id - 1][i] == pid) {
                auto [daa_offset, daa_size] = FetchDAABounds(spo_, sid);
                return AccessDAA(spo_, i, daa_offset, daa_size);
            }
        }
    }
    return std::make_shared<std::vector<uint>>();
}

// ?s p o
std::shared_ptr<std::vector<uint>> IndexRetriever::GetByOP(uint oid, uint pid) {
    if ((0 < oid && oid <= dict_.shared_cnt()) || max_subject_id_ < oid) {
        if (oid > dict_.shared_cnt())
            oid -= dict_.subject_cnt();

        uint c_set_id = AccessToDAA(ops_, (oid - 1) * 2);
        for (uint i = 0; i < object_characteristic_set_[c_set_id - 1].size(); i++) {
            if (object_characteristic_set_[c_set_id - 1][i] == pid) {
                auto [daa_offset, daa_size] = FetchDAABounds(ops_, oid);
                return AccessDAA(ops_, i, daa_offset, daa_size);
            }
        }
    }
    return std::make_shared<std::vector<uint>>();
}

// s ?p o
std::shared_ptr<std::vector<uint>> IndexRetriever::GetBySO(uint sid, uint oid) {
    std::shared_ptr<std::vector<uint>> result = std::make_shared<std::vector<uint>>();

    // 796
    // <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>
    if ((0 < sid && sid <= max_subject_id_) && (oid <= dict_.shared_cnt() || max_subject_id_ < oid)) {
        uint ops_daa_offset = (oid - 1) * 2;
        if (oid > dict_.shared_cnt())
            ops_daa_offset = (oid - dict_.subject_cnt() - 1) * 2;

        uint s_c_set_id = AccessToDAA(spo_, (sid - 1) * 2);
        uint o_c_set_id = AccessToDAA(ops_, ops_daa_offset);
        std::vector<uint>& s_c_set = subject_characteristic_set_[s_c_set_id - 1];
        std::vector<uint>& o_c_set = object_characteristic_set_[o_c_set_id - 1];

        for (uint i = 0; i < s_c_set.size(); i++) {
            for (uint j = 0; j < o_c_set.size(); j++) {
                if (s_c_set[i] == o_c_set[j]) {
                    auto r = GetBySP(sid, s_c_set[i]);
                    bool found = std::binary_search(r->begin(), r->end(), oid);
                    if (found)
                        result->push_back(s_c_set[i]);
                }
            }
        }
    }

    return result;
}

std::shared_ptr<std::vector<uint>> IndexRetriever::GetByS(uint sid) {
    std::shared_ptr<std::vector<uint>> result = std::make_shared<std::vector<uint>>();

    if (0 < sid && sid <= max_subject_id_) {
        auto [daa_offset, daa_size] = FetchDAABounds(spo_, sid);
        result = AccessAllArrays(spo_, daa_offset, daa_size);
        std::sort(result->begin(), result->end());
        result->erase(std::unique(result->begin(), result->end()), result->end());
        return result;
    }
    return result;
}

std::shared_ptr<std::vector<uint>> IndexRetriever::GetByO(uint oid) {
    std::shared_ptr<std::vector<uint>> result = std::make_shared<std::vector<uint>>();

    if ((0 < oid && oid <= dict_.shared_cnt()) || max_subject_id_ < oid) {
        if (oid > dict_.shared_cnt())
            oid -= dict_.subject_cnt();

        auto [daa_offset, daa_size] = FetchDAABounds(ops_, oid);
        result = AccessAllArrays(ops_, daa_offset, daa_size);
        std::sort(result->begin(), result->end());
        result->erase(std::unique(result->begin(), result->end()), result->end());
    }
    return result;
}

uint IndexRetriever::GetSSetSize(uint pid) {
    if (ps_sets_[pid - 1]->size() == 0) {
        uint s_array_size;
        if (predicate_index_compressed_) {
            s_array_size = predicate_index_[(pid - 1) * 4 + 1];
        } else {
            uint s_array_offset = predicate_index_[(pid - 1) * 2];
            s_array_size = predicate_index_[(pid - 1) * 2 + 1] - s_array_offset;
        }
        return s_array_size;
    }

    return ps_sets_[pid - 1]->size();
}

uint IndexRetriever::GetOSetSize(uint pid) {
    if (po_sets_[pid - 1]->size() == 0) {
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
    return po_sets_[pid - 1]->size();
}

uint IndexRetriever::GetBySSize(uint sid) {
    if (sid <= max_subject_id_) {
        auto [daa_offset, daa_size] = FetchDAABounds(spo_, sid);
        return daa_size;
    }
    return 0;
}

uint IndexRetriever::GetByOSize(uint oid) {
    if ((0 < oid && oid <= dict_.shared_cnt()) || max_subject_id_ < oid) {
        auto [daa_offset, daa_size] = FetchDAABounds(ops_, oid);
        return daa_size;
    }
    return 0;
}

uint IndexRetriever::GetBySPSize(uint sid, uint pid) {
    return GetBySP(sid, pid)->size();
}

uint IndexRetriever::GetByOPSize(uint oid, uint pid) {
    return GetByOP(oid, pid)->size();
}

uint IndexRetriever::GetBySOSize(uint sid, uint oid) {
    return GetBySO(sid, oid)->size();
}

uint IndexRetriever::predicate_cnt() {
    return dict_.predicate_cnt();
}

uint IndexRetriever::shared_cnt() {
    return dict_.shared_cnt();
}
