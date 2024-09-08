#include "rdf-tdaa/index/index_builder.hpp"

void IndexBuilder::PredicateIndex::Build(std::vector<std::pair<uint, uint>>& so_pairs) {
    for (const auto& so : so_pairs) {
        s_set_.insert(so.first);
        o_set_.insert(so.second);
    }
}

void IndexBuilder::PredicateIndex::Clear() {
    phmap::btree_set<uint>().swap(s_set_);
    phmap::btree_set<uint>().swap(o_set_);
}

IndexBuilder::IndexBuilder(std::string db_name, std::string data_file) {
    db_name_ = db_name;
    data_file_ = data_file;
    db_index_path_ = "./DB_DATA_ARCHIVE/" + db_name_ + "/index/";

    fs::path db_path = db_index_path_;
    if (!fs::exists(db_path)) {
        fs::create_directories(db_path);
    }

    db_dictionary_path_ = "./DB_DATA_ARCHIVE/" + db_name_ + "/dictionary/";

    fs::path dict_path = db_dictionary_path_;
    if (!fs::exists(dict_path)) {
        fs::create_directories(dict_path);
    }

    pso_ = std::make_shared<hash_map<uint, std::vector<std::pair<uint, uint>>>>();
}

void IndexBuilder::BuildPredicateIndex(std::vector<PredicateIndex>& predicate_indexes) {
    std::vector<std::pair<uint, uint>> predicate_rank;
    for (uint pid = 1; pid <= dict_.predicate_cnt(); pid++) {
        pso_->at(pid).shrink_to_fit();
        uint i = 0, size = pso_->at(pid).size();
        for (; i < predicate_rank.size(); i++) {
            if (predicate_rank[i].second <= size)
                break;
        }
        predicate_rank.insert(predicate_rank.begin() + i, {pid, size});
    }

    std::deque<uint> task_queue;
    std::mutex task_queue_mutex;
    std::condition_variable task_queue_cv;
    std::atomic<bool> task_queue_empty{false};

    uint cpu_count = std::thread::hardware_concurrency();

    for (uint i = 0; i < dict_.predicate_cnt(); i++)
        task_queue.push_back(predicate_rank[i].first);

    std::vector<std::thread> threads;
    for (uint tid = 0; tid < cpu_count; tid++) {
        threads.emplace_back(std::bind(&IndexBuilder::SubBuildPredicateIndex, this, &task_queue,
                                       &task_queue_mutex, &task_queue_cv, &task_queue_empty,
                                       &predicate_indexes));
    }
    for (auto& t : threads)
        t.join();
}

void IndexBuilder::SubBuildPredicateIndex(std::deque<uint>* task_queue,
                                          std::mutex* task_queue_mutex,
                                          std::condition_variable* task_queue_cv,
                                          std::atomic<bool>* task_queue_empty,
                                          std::vector<PredicateIndex>* predicate_indexes) {
    while (true) {
        std::unique_lock<std::mutex> lock(*task_queue_mutex);
        while (task_queue->empty() && !task_queue_empty->load())
            task_queue_cv->wait(lock);
        if (task_queue->empty())
            break;  // No more tasks

        uint pid = task_queue->front();
        task_queue->pop_front();
        if (task_queue->empty()) {
            task_queue_empty->store(true);
        }
        lock.unlock();

        predicate_indexes->at(pid - 1).Build(pso_->at(pid));
    }
}

void IndexBuilder::StorePredicateIndexNoCompress(std::vector<PredicateIndex>& predicate_indexes) {
    auto beg = std::chrono::high_resolution_clock::now();

    ulong predicate_index_arrays_file_size = 0;
    for (uint pid = 1; pid <= dict_.predicate_cnt(); pid++) {
        predicate_index_arrays_file_size += ulong(predicate_indexes[pid - 1].s_set_.size() * 4ul);
        predicate_index_arrays_file_size += ulong(predicate_indexes[pid - 1].o_set_.size() * 4ul);
    }
    MMap<uint> predicate_index_ =
        MMap<uint>(db_index_path_ + "predicate_index", dict_.predicate_cnt() * 2 * 4);
    MMap<uint> predicate_index_arrays =
        MMap<uint>(db_index_path_ + "predicate_index_arrays", predicate_index_arrays_file_size);

    ulong arrays_file_offset = 0;
    phmap::btree_set<uint>* ps_set;
    phmap::btree_set<uint>* po_set;
    for (uint pid = 1; pid <= dict_.predicate_cnt(); pid++) {
        ps_set = &predicate_indexes[pid - 1].s_set_;
        po_set = &predicate_indexes[pid - 1].o_set_;

        predicate_index_[(pid - 1) * 2] = arrays_file_offset;
        for (auto it = ps_set->begin(); it != ps_set->end(); it++) {
            predicate_index_arrays[arrays_file_offset] = *it;
            arrays_file_offset++;
        }

        predicate_index_[(pid - 1) * 2 + 1] = arrays_file_offset;
        for (auto it = po_set->begin(); it != po_set->end(); it++) {
            predicate_index_arrays[arrays_file_offset] = *it;
            arrays_file_offset++;
        }

        predicate_indexes[pid - 1].Clear();
    }

    predicate_index_.CloseMap();
    predicate_index_arrays.CloseMap();

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> diff = end - beg;
    std::cout << "store predicate index takes " << diff.count() << " ms.               " << std::endl;
}

void IndexBuilder::StorePredicateIndex(std::vector<PredicateIndex>& predicate_indexes) {
    ulong predicate_index_file_size_ = dict_.predicate_cnt() * 4 * 4;

    MMap<uint> predicate_index = MMap<uint>(db_index_path_ + "predicate_index", predicate_index_file_size_);

    ulong total_compressed_size = 0;
    std::vector<std::pair<uint8_t*, uint>> compressed_ps_set(dict_.predicate_cnt());
    std::vector<std::pair<uint8_t*, uint>> compressed_po_set(dict_.predicate_cnt());

    phmap::btree_set<uint>* ps_set;
    phmap::btree_set<uint>* po_set;
    uint* set_buffer;
    uint buffer_offset;
    uint8_t* compressed_buffer;
    ulong compressed_size;
    uint last;
    for (uint pid = 1; pid <= dict_.predicate_cnt(); pid++) {
        ps_set = &predicate_indexes[pid - 1].s_set_;
        po_set = &predicate_indexes[pid - 1].o_set_;

        buffer_offset = 0;
        last = 0;
        set_buffer = new uint[ps_set->size()];
        for (auto it = ps_set->begin(); it != ps_set->end(); it++) {
            set_buffer[buffer_offset] = *it - last;
            last = *it;
            buffer_offset++;
        }
        compressed_buffer = new uint8_t[streamvbyte_max_compressedbytes(buffer_offset)];
        compressed_size = streamvbyte_encode(set_buffer, buffer_offset, compressed_buffer);
        predicate_index[(pid - 1) * 4 + 1] = ps_set->size();
        compressed_ps_set[pid - 1] = {compressed_buffer, compressed_size};
        total_compressed_size += compressed_size;
        delete[] set_buffer;

        buffer_offset = 0;
        last = 0;
        set_buffer = new uint[po_set->size()];
        for (auto it = po_set->begin(); it != po_set->end(); it++) {
            set_buffer[buffer_offset] = *it - last;
            last = *it;
            buffer_offset++;
        }
        compressed_buffer = new uint8_t[streamvbyte_max_compressedbytes(buffer_offset)];
        compressed_size = streamvbyte_encode(set_buffer, buffer_offset, compressed_buffer);
        predicate_index[(pid - 1) * 4 + 3] = po_set->size();
        compressed_po_set[pid - 1] = {compressed_buffer, compressed_size};
        total_compressed_size += compressed_size;
        delete[] set_buffer;

        predicate_indexes[pid - 1].Clear();
    }

    MMap<uint8_t> predicate_index_arrays =
        MMap<uint8_t>(db_index_path_ + "predicate_index_arrays", total_compressed_size);
    for (uint pid = 1; pid <= dict_.predicate_cnt(); pid++) {
        predicate_index[(pid - 1) * 4] = predicate_index_arrays.offset_;
        for (uint i = 0; i < compressed_ps_set[pid - 1].second; i++)
            predicate_index_arrays.Write(compressed_ps_set[pid - 1].first[i]);
        delete[] compressed_ps_set[pid - 1].first;

        predicate_index[(pid - 1) * 4 + 2] = predicate_index_arrays.offset_;
        for (uint i = 0; i < compressed_po_set[pid - 1].second; i++)
            predicate_index_arrays.Write(compressed_po_set[pid - 1].first[i]);
        delete[] compressed_po_set[pid - 1].first;
    }
    std::vector<std::pair<uint8_t*, uint>>().swap(compressed_ps_set);
    std::vector<std::pair<uint8_t*, uint>>().swap(compressed_po_set);

    predicate_index.CloseMap();
    predicate_index_arrays.CloseMap();
}

void IndexBuilder::BuildCharacteristicSet(std::vector<std::pair<uint, uint>>& to_set_id, Order order) {
    uint entity_cnt;
    if (order == Order::kSPO)
        entity_cnt = dict_.shared_cnt() + dict_.subject_cnt();
    else
        entity_cnt = dict_.shared_cnt() + dict_.object_cnt();

    std::vector<std::vector<uint>> predicate_sets = std::vector<std::vector<uint>>(entity_cnt);
    to_set_id = std::vector<std::pair<uint, uint>>(entity_cnt);

    // build predicate_sets
    for (uint pid = 1; pid <= dict_.predicate_cnt(); pid++) {
        for (auto it = pso_->at(pid).begin(); it != pso_->at(pid).end(); it++) {
            uint id;
            if (order == Order::kSPO) {
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

    // using dict trie to unique sets
    PredicateSetTrie trie = PredicateSetTrie();
    uint max_id = 0;
    uint present_id;
    ulong serialize_size = 0;
    std::vector<uint> unique_offset;
    for (uint i = 0; i < predicate_sets.size(); i++) {
        present_id = trie.insert(predicate_sets[i]);
        to_set_id[i] = {present_id, predicate_sets[i].size()};
        if (present_id > max_id) {
            serialize_size += predicate_sets[i].size() + 1;
            unique_offset.push_back(i);
            max_id = present_id;
        }
    }
    // std::cout << predicate_sets.size() << std::endl;
    // std::cout << trie.set_cnt_ - 1 << std::endl;

    // prepare for serializing
    uint* serialize_data = new uint[serialize_size];
    ulong offset = 0;
    for (uint i = 0; i < unique_offset.size(); i++) {
        std::vector<uint>& set = predicate_sets[unique_offset[i]];
        std::copy(set.begin(), set.end(), serialize_data + offset);
        offset += set.size();
        serialize_data[offset++] = 0;
    }

    std::string file_name = (order == Order::kSPO) ? "s_c_sets" : "o_c_sets";

    // serializing
    CompressAndSave(serialize_data, serialize_size, db_index_path_ + file_name);
    delete[] serialize_data;
    std::vector<std::vector<uint>>().swap(predicate_sets);
    trie.~PredicateSetTrie();
}

void IndexBuilder::CompressAndSave(uint* data, ulong size, std::string filename) {
    uint max_compressed_length = streamvbyte_max_compressedbytes(size);
    uint8_t* compressed_buffer = new uint8_t[max_compressed_length];

    if (size > UINT_MAX)
        std::cout << "error size: " << size << std::endl;
    uint compressed_length = streamvbyte_encode(data, size, compressed_buffer);  // encoding

    std::ofstream outfile(filename, std::ios::binary);

    outfile.write(reinterpret_cast<const char*>(&size), sizeof(size));
    outfile.write(reinterpret_cast<const char*>(&compressed_length), sizeof(compressed_length));
    outfile.write(reinterpret_cast<const char*>(compressed_buffer), compressed_length);
    outfile.close();

    delete[] compressed_buffer;
}

uint IndexBuilder::BuildEntitySets(std::vector<std::pair<uint, uint>>& to_set_id,
                                   std::vector<std::vector<std::vector<uint>>>& entity_set,
                                   Order order) {
    uint entity_cnt = to_set_id.size();
    // (s, p) or (o, p) 's o/s set
    entity_set.reserve(entity_cnt);
    for (uint i = 0; i < entity_cnt; i++)
        entity_set.push_back(std::vector<std::vector<uint>>(to_set_id[i].second));

    std::vector<uint> p_offset = std::vector<uint>(entity_cnt);

    Bitset bitset;
    for (uint pid = 1; pid <= dict_.predicate_cnt(); pid++) {
        bitset = Bitset(pso_->at(pid).size() / 2);
        for (auto it = pso_->at(pid).begin(); it != pso_->at(pid).end(); it++) {
            auto [sid, oid] = *it;
            if (order == Order::kSPO) {
                if (bitset.Get(sid))
                    entity_set[sid - 1][p_offset[sid - 1] - 1].push_back(oid);
                else {
                    entity_set[sid - 1][p_offset[sid - 1]].push_back(oid);
                    bitset.Set(sid);
                    p_offset[sid - 1]++;
                }
            } else {
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

    ulong distinct_triple_cnt = 0;
    std::vector<uint>().swap(p_offset);
    uint max = 0;
    for (uint id = 1; id <= entity_cnt; id++) {
        for (uint p = 0; p < entity_set[id - 1].size(); p++) {
            auto& set = entity_set[id - 1][p];
            std::sort(set.begin(), set.end());
            set.erase(std::unique(set.begin(), set.end()), set.end());
            distinct_triple_cnt += set.size();
            if (set[0] > max)
                max = set[0];
            uint last = 0;
            for (uint i = 0; i < set.size() - 1; i++) {
                last += set[i];
                set[i + 1] -= last;
                if (set[i + 1] > max)
                    max = set[i + 1];
            }
        }
    }
    if (all_arr_size_ == 0) {
        all_arr_size_ = distinct_triple_cnt;
        
    } else if (all_arr_size_ != distinct_triple_cnt)
        perror("error !!!");
    return std::ceil(std::log2(max));
}

void IndexBuilder::BuildAndSaveIndex(std::vector<std::pair<uint, uint>>& to_set_id,
                                     std::vector<std::vector<std::vector<uint>>>& entity_set,
                                     uint levels_width,
                                     Order order) {
    // std::cout << levels_width << std::endl;
    ulong entity_cnt = entity_set.size();
    std::string prefix = (order == Order::kSPO) ? "spo" : "ops";

    MMap<uint> daa_levels;
    ulong file_size;
    if (compress_levels_)
        file_size = ulong(all_arr_size_ * ulong(levels_width) + 7ul) / 8ul;
    else
        file_size = all_arr_size_ * 4;
    daa_levels = MMap<uint>(db_index_path_ + prefix + "_daa_levels", file_size);

    std::cout << all_arr_size_ << " " << ulong(all_arr_size_ + 7ul) / 8ul << std::endl;
    MMap<char> daa_level_end =
        MMap<char>(db_index_path_ + prefix + "_daa_level_end", ulong(all_arr_size_ + 7ul) / 8ul);
    MMap<char> daa_array_end =
        MMap<char>(db_index_path_ + prefix + "_daa_array_end", ulong(all_arr_size_ + 7ul) / 8ul);

    uint daa_file_offset = 0;

    char level_end_buffer = 0;
    char array_end_buffer = 0;
    uint end_buffer_offset = 7;
    uint levels_buffer = 0;
    uint levels_buffer_offset = 31;

    uint max_characteristic_set_id = 0;
    std::vector<uint> daa_offsets(entity_cnt);

    for (uint id = 1; id <= entity_cnt; id++) {
        DAA daa = DAA(entity_set[id - 1]);

        daa_file_offset += daa.data_cnt_;
        daa_offsets[id - 1] = daa_file_offset;

        if (to_set_id[id - 1].first > max_characteristic_set_id)
            max_characteristic_set_id = to_set_id[id - 1].first;

        for (uint levels_offset = 0; levels_offset < daa.data_cnt_; levels_offset++) {
            if (compress_levels_) {
                for (uint i = 0; i < levels_width; i++) {
                    if (daa.levels_[levels_offset] & (1 << (levels_width - 1 - i)))
                        levels_buffer |= 1 << levels_buffer_offset;
                    if (levels_buffer_offset == 0) {
                        daa_levels.Write(levels_buffer);
                        levels_buffer = 0;
                        levels_buffer_offset = 32;
                    }
                    levels_buffer_offset--;
                }
            } else {
                daa_levels.Write(daa.levels_[levels_offset]);
            }

            if (daa.level_end_[levels_offset / 8] & (1 << (7 - levels_offset % 8)))
                level_end_buffer |= 1 << end_buffer_offset;
            if (daa.array_end_[levels_offset / 8] & (1 << (7 - levels_offset % 8)))
                array_end_buffer |= 1 << end_buffer_offset;
            if (end_buffer_offset == 0) {
                daa_level_end.Write(level_end_buffer);
                daa_array_end.Write(array_end_buffer);
                level_end_buffer = 0;
                array_end_buffer = 0;
                end_buffer_offset = 8;
            }
            end_buffer_offset--;
        }
    }
    if (levels_buffer != 0)
        daa_levels.Write(levels_buffer);
    if (level_end_buffer != 0) {
        daa_level_end.Write(level_end_buffer);
        daa_array_end.Write(array_end_buffer);
    }

    daa_levels.CloseMap();
    daa_level_end.CloseMap();
    daa_array_end.CloseMap();

    uint chara_set_id_width;
    uint daa_offset_width;
    if (compress_to_daa_ || compress_levels_) {
        chara_set_id_width = std::ceil(std::log2(max_characteristic_set_id));
        daa_offset_width = std::ceil(std::log2(dict_.triplet_cnt()));

        MMap<uint> data_width = MMap<uint>(db_index_path_ + "data_width", 6 * 4);
        data_width[(order == Order::kSPO) ? 0 : 3] = chara_set_id_width;
        data_width[(order == Order::kSPO) ? 1 : 4] = daa_offset_width;
        data_width[(order == Order::kSPO) ? 2 : 5] = levels_width;
        data_width.CloseMap();
    }

    if (compress_to_daa_) {
        file_size =
            ulong(ulong(ulong(ulong(chara_set_id_width) + ulong(daa_offset_width)) * entity_cnt) + 7ul) / 8ul;
        MMap<uint> to_daa = MMap<uint>(db_index_path_ + prefix + "_to_daa", file_size);

        ulong to_daa_buffer = 0;
        uint to_daa_buffer_offset = 31;
        for (uint id = 1; id <= entity_cnt; id++) {
            for (uint i = 0; i < chara_set_id_width + daa_offset_width; i++) {
                bool bit;
                if (i < chara_set_id_width)
                    bit = to_set_id[id - 1].first & (1ull << (chara_set_id_width - 1 - i));
                else
                    bit = daa_offsets[id - 1] & (1ull << (daa_offset_width - 1 - (i - chara_set_id_width)));
                if (bit)
                    to_daa_buffer |= 1ull << to_daa_buffer_offset;

                if (to_daa_buffer_offset == 0) {
                    to_daa.Write(to_daa_buffer);
                    to_daa_buffer = 0;
                    to_daa_buffer_offset = 32;
                }
                to_daa_buffer_offset--;
            }
        }
        if (to_daa_buffer != 0)
            to_daa.Write(to_daa_buffer);

        to_daa.CloseMap();
    } else {
        MMap<uint> to_daa = MMap<uint>(db_index_path_ + prefix + "_to_daa", ulong(entity_cnt * 2ul * 4ul));
        for (uint id = 1; id <= entity_cnt; id++) {
            to_daa[(id - 1) * 2] = to_set_id[id - 1].first;
            to_daa[(id - 1) * 2 + 1] = daa_offsets[id - 1];
        }
        to_daa.CloseMap();
    }
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

    // for (uint pid = 1; pid <= dict_.predicate_cnt(); pid++) {
    //     std::cout << "-------" << pid << "-------" << std::endl;
    //     for (auto it = pso_->at(pid).begin(); it != pso_->at(pid).end(); it++) {
    //         auto [sid, oid] = *it;
    //         std::cout << sid << " " << oid << std::endl;
    //     }
    // }

    beg = std::chrono::high_resolution_clock::now();
    std::vector<PredicateIndex> predicate_indexes(dict_.predicate_cnt());
    BuildPredicateIndex(predicate_indexes);
    if (compress_predicate_index_)
        StorePredicateIndex(predicate_indexes);
    else
        StorePredicateIndexNoCompress(predicate_indexes);
    end = std::chrono::high_resolution_clock::now();
    diff = end - beg;
    std::cout << "build predicate index takes " << diff.count() << " ms." << std::endl;

    beg = std::chrono::high_resolution_clock::now();
    std::vector<std::pair<uint, uint>> subject_to_set_id;
    std::vector<std::pair<uint, uint>> object_to_set_id;
    std::thread s_t(
        std::bind(&IndexBuilder::BuildCharacteristicSet, this, std::ref(subject_to_set_id), Order::kSPO));
    std::thread o_t(
        std::bind(&IndexBuilder::BuildCharacteristicSet, this, std::ref(object_to_set_id), Order::kOPS));
    s_t.join();
    o_t.join();
    malloc_trim(0);
    end = std::chrono::high_resolution_clock::now();
    diff = end - beg;
    std::cout << "build characteristic set takes " << diff.count() << " ms." << std::endl;

    beg = std::chrono::high_resolution_clock::now();
    std::vector<std::vector<std::vector<uint>>> spo_entity_set;
    uint levels_width = BuildEntitySets(subject_to_set_id, spo_entity_set, Order::kSPO);
    BuildAndSaveIndex(subject_to_set_id, spo_entity_set, levels_width, Order::kSPO);
    std::vector<std::vector<std::vector<uint>>>().swap(spo_entity_set);
    end = std::chrono::high_resolution_clock::now();
    diff = end - beg;
    std::cout << "build spo index takes " << diff.count() << " ms." << std::endl;

    beg = std::chrono::high_resolution_clock::now();
    std::vector<std::vector<std::vector<uint>>> ops_entity_set;
    levels_width = BuildEntitySets(object_to_set_id, ops_entity_set, Order::kOPS);
    BuildAndSaveIndex(object_to_set_id, ops_entity_set, levels_width, Order::kOPS);
    std::vector<std::vector<std::vector<uint>>>().swap(ops_entity_set);
    end = std::chrono::high_resolution_clock::now();
    diff = end - beg;
    std::cout << "build ops index takes " << diff.count() << " ms." << std::endl;

    return true;
}