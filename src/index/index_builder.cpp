#include "rdf-tdaa/index/index_builder.hpp"
#include "rdf-tdaa/dictionary/dictionary_builder.hpp"
#include "rdf-tdaa/index/characteristic_set.hpp"
#include "rdf-tdaa/index/cs_daa_map.hpp"
#include "rdf-tdaa/index/daas.hpp"
#include "rdf-tdaa/utils/bitset.hpp"
#include "rdf-tdaa/utils/mmap.hpp"
#include "rdf-tdaa/utils/vbyte.hpp"
#include "streamvbyte.h"

IndexBuilder::IndexBuilder(std::string db_name, std::string data_file) {
    db_name_ = db_name;
    data_file_ = data_file;
    db_index_path_ = "./DB_DATA_ARCHIVE/" + db_name_ + "/index/";
    spo_index_path_ = db_index_path_ + "spo/";
    ops_index_path_ = db_index_path_ + "ops/";
    db_dictionary_path_ = "./DB_DATA_ARCHIVE/" + db_name_ + "/dictionary/";

    fs::path db_path = db_index_path_;
    if (!fs::exists(db_path))
        fs::create_directories(db_path);
    fs::path spo_path = spo_index_path_;
    if (!fs::exists(spo_path))
        fs::create_directories(spo_path);
    fs::path ops_path = ops_index_path_;
    if (!fs::exists(ops_path))
        fs::create_directories(ops_path);
    fs::path dict_path = db_dictionary_path_;
    if (!fs::exists(dict_path))
        fs::create_directories(dict_path);

    pso_ = std::make_shared<hash_map<uint, std::vector<std::pair<uint, uint>>>>();
}

void IndexBuilder::BuildCharacteristicSet(std::vector<uint>& c_set_id,
                                          std::vector<uint>& c_set_size,
                                          Permutation permutation) {
    uint entity_cnt;
    if (permutation == Permutation::kSPO)
        entity_cnt = dict_.shared_cnt() + dict_.subject_cnt();
    else
        entity_cnt = dict_.shared_cnt() + dict_.object_cnt();

    std::vector<std::vector<uint>> predicate_sets = std::vector<std::vector<uint>>(entity_cnt);

    // build predicate_sets
    for (uint pid = 1; pid <= dict_.predicate_cnt(); pid++) {
        for (auto it = pso_->at(pid).begin(); it != pso_->at(pid).end(); it++) {
            uint id;
            if (permutation == Permutation::kSPO) {
                id = it->first;
            } else {
                id = it->second;
                if (id > dict_.shared_cnt())
                    id -= dict_.subject_cnt();
            }
            if (predicate_sets[id - 1].size() == 0 || predicate_sets[id - 1].back() != pid)
                predicate_sets[id - 1].push_back(pid);
        }
    }

    CharacteristicSet::Trie trie = CharacteristicSet::Trie();
    uint max_id = 0;
    uint present_id;

    std::vector<std::pair<uint8_t*, uint>> compressed_sets;
    std::vector<uint> original_size;

    c_set_id = std::vector<uint>(entity_cnt);
    c_set_size = std::vector<uint>(entity_cnt);
    for (uint set_id = 0; set_id < predicate_sets.size(); set_id++) {
        present_id = trie.Insert(predicate_sets[set_id]);
        c_set_id[set_id] = present_id;
        c_set_size[set_id] = predicate_sets[set_id].size();
        if (present_id > max_id) {
            max_id = present_id;
            uint last = 0;
            for (uint i = 0; i < predicate_sets[set_id].size() - 1; i++) {
                last += predicate_sets[set_id][i];
                predicate_sets[set_id][i + 1] -= last;
            }
            auto compressed_set = Compress(predicate_sets[set_id].data(), predicate_sets[set_id].size());
            compressed_sets.push_back(compressed_set);
            original_size.push_back(predicate_sets[set_id].size());
        }
    }
    trie.~Trie();

    std::vector<std::vector<uint>>().swap(predicate_sets);

    std::string file_name = (permutation == Permutation::kSPO) ? "s_c_sets" : "o_c_sets";
    CharacteristicSet c_set = CharacteristicSet(db_index_path_ + file_name);
    c_set.Build(compressed_sets, original_size);
}

void IndexBuilder::BuildEntitySets(PredicateIndex& predicate_index,
                                   std::vector<uint>& c_set_size,
                                   std::vector<std::vector<std::vector<uint>>>& entity_set,
                                   Permutation permutation) {
    uint entity_cnt = c_set_size.size();
    // (s, p) or (o, p) 's o/s set
    entity_set.reserve(entity_cnt);
    for (uint i = 0; i < entity_cnt; i++)
        entity_set.push_back(std::vector<std::vector<uint>>(c_set_size[i]));

    std::vector<uint> p_offset = std::vector<uint>(entity_cnt);

    Bitset bitset;
    for (uint pid = 1; pid <= dict_.predicate_cnt(); pid++) {
        bitset = Bitset(pso_->at(pid).size() / 2);
        for (auto it = pso_->at(pid).begin(); it != pso_->at(pid).end(); it++) {
            auto [sid, oid] = *it;
            if (permutation == Permutation::kSPO) {
                oid = predicate_index.index_[pid - 1].oid2offset[oid];
                if (bitset.Get(sid))
                    entity_set[sid - 1][p_offset[sid - 1] - 1].push_back(oid);
                else {
                    entity_set[sid - 1][p_offset[sid - 1]].push_back(oid);
                    bitset.Set(sid);
                    p_offset[sid - 1]++;
                }
            } else {
                sid = predicate_index.index_[pid - 1].sid2offset[sid];
                if (oid > dict_.shared_cnt())
                    oid -= dict_.subject_cnt();
                if (bitset.Get(oid))
                    entity_set[oid - 1][p_offset[oid - 1] - 1].push_back(sid);
                else {
                    entity_set[oid - 1][p_offset[oid - 1]].push_back(sid);
                    bitset.Set(oid);
                    p_offset[oid - 1]++;
                }
            }
        }
    }
    std::vector<uint>().swap(p_offset);
}

bool IndexBuilder::Build() {
    std::cout << "Indexing ..." << std::endl;

    auto beg = std::chrono::high_resolution_clock::now();

    DictionaryBuilder dict_builder = DictionaryBuilder(db_dictionary_path_, data_file_);
    dict_builder.Build();
    dict_builder.EncodeRDF(*pso_);
    dict_builder.Close();
    dict_ = Dictionary(db_dictionary_path_);
    dict_.Close();
    malloc_trim(0);

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> diff = end - beg;
    std::cout << "build dictionary takes " << diff.count() << " ms." << std::endl;

    beg = std::chrono::high_resolution_clock::now();
    PredicateIndex predicate_index = PredicateIndex(pso_, db_index_path_, dict_.predicate_cnt());
    predicate_index.Build();
    predicate_index.Store();
    end = std::chrono::high_resolution_clock::now();
    diff = end - beg;
    std::cout << "build predicate index takes " << diff.count() << " ms." << std::endl;

    beg = std::chrono::high_resolution_clock::now();

    std::vector<uint> subject_cs_id;
    std::vector<uint> subject_cs_size;
    std::vector<uint> object_cs_id;
    std::vector<uint> object_cs_size;
    std::thread s_t(std::bind(&IndexBuilder::BuildCharacteristicSet, this, std::ref(subject_cs_id),
                              std::ref(subject_cs_size), Permutation::kSPO));
    std::thread o_t(std::bind(&IndexBuilder::BuildCharacteristicSet, this, std::ref(object_cs_id),
                              std::ref(object_cs_size), Permutation::kOPS));
    s_t.join();
    o_t.join();
    malloc_trim(0);

    end = std::chrono::high_resolution_clock::now();
    diff = end - beg;
    std::cout << "build characteristic set takes " << diff.count() << " ms." << std::endl;

    beg = std::chrono::high_resolution_clock::now();
    std::vector<std::vector<std::vector<uint>>> spo_entity_set;
    BuildEntitySets(predicate_index, subject_cs_size, spo_entity_set, Permutation::kSPO);
    DAAs spo_daas = DAAs(spo_index_path_);
    spo_daas.Build(spo_entity_set);
    std::vector<std::vector<std::vector<uint>>>().swap(spo_entity_set);
    end = std::chrono::high_resolution_clock::now();
    diff = end - beg;
    std::cout << "build spo index takes " << diff.count() << " ms." << std::endl;

    beg = std::chrono::high_resolution_clock::now();
    std::vector<std::vector<std::vector<uint>>> ops_entity_set;
    BuildEntitySets(predicate_index, object_cs_size, ops_entity_set, Permutation::kOPS);
    DAAs ops_daas = DAAs(ops_index_path_);
    ops_daas.Build(ops_entity_set);
    std::vector<std::vector<std::vector<uint>>>().swap(ops_entity_set);
    end = std::chrono::high_resolution_clock::now();
    diff = end - beg;
    std::cout << "build ops index takes " << diff.count() << " ms." << std::endl;

    std::pair<std::vector<uint>&, std::vector<ulong>&> spo_map = {subject_cs_id, spo_daas.daa_offsets()};
    std::pair<std::vector<uint>&, std::vector<ulong>&> ops_map = {object_cs_id, ops_daas.daa_offsets()};

    beg = std::chrono::high_resolution_clock::now();
    CsDaaMap cs_daa_map = CsDaaMap(db_index_path_);
    cs_daa_map.Build(spo_map, ops_map);
    end = std::chrono::high_resolution_clock::now();
    diff = end - beg;
    std::cout << "build cs daa map takes " << diff.count() << " ms." << std::endl;

    std::pair<uint, uint> cs_id_width = cs_daa_map.cs_id_width();
    std::pair<uint, uint> daa_offset_width = cs_daa_map.daa_offset_width();

    MMap<uint> metadata = MMap<uint>(db_index_path_ + "metadata", 9 * 4);
    metadata[0] = cs_daa_map.shared_id_size();
    metadata[1] = cs_id_width.first;
    metadata[2] = cs_id_width.second;
    metadata[3] = daa_offset_width.first;
    metadata[4] = daa_offset_width.second;
    metadata[5] = cs_daa_map.not_shared_cs_id_width();
    metadata[6] = cs_daa_map.not_shared_daa_offset_width();
    metadata[7] = spo_daas.daa_levels_width();
    metadata[8] = ops_daas.daa_levels_width();
    metadata.CloseMap();

    return true;
}