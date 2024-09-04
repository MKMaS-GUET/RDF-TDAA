#include "rdf-tdaa/dictionary/dictionary_builder.hpp"

DictionaryBuilder::DictionaryBuilder(std::string& dict_path, std::string& file_path)
    : dict_path_(dict_path), file_path_(file_path) {}

void DictionaryBuilder::Init() {
    std::filesystem::path subjects_path = dict_path_ + "/subjects";
    std::filesystem::path objects_path = dict_path_ + "/objects";
    std::filesystem::path shared_path = dict_path_ + "/shared";

    if (!std::filesystem::exists(subjects_path))
        std::filesystem::create_directories(subjects_path);
    if (!std::filesystem::exists(objects_path))
        std::filesystem::create_directories(objects_path);
    if (!std::filesystem::exists(shared_path))
        std::filesystem::create_directories(shared_path);
}

void DictionaryBuilder::BuildDict() {
    uint temp_node_id = 1;
    uint sid = 0, oid = 0;
    hash_map<std::string, uint>::iterator s_it, o_it, shared_it;
    std::string s, p, o;
    std::ifstream fin(file_path_, std::ios::in);

    std::cout << "assigning id to nodes." << std::endl;
    while (fin >> s >> p) {
        fin.ignore();
        std::getline(fin, o);
        for (o.pop_back(); o.back() == ' ' || o.back() == '.'; o.pop_back()) {
        }

        shared_it = shared_.find(s);
        if (shared_it == shared_.end()) {
            o_it = objects_.find(s);
            if (o_it != objects_.end()) {
                sid = o_it->second;
                shared_.insert({s, sid});
                objects_.erase(s);
            } else {
                auto ret = subjects_.insert({s, temp_node_id});
                if (ret.second) {
                    sid = temp_node_id;
                    temp_node_id++;
                } else {
                    sid = ret.first->second;
                }
            }
        } else {
            sid = shared_it->second;
        }

        shared_it = shared_.find(o);
        if (shared_it == shared_.end()) {
            s_it = subjects_.find(o);
            if (s_it != subjects_.end()) {
                oid = s_it->second;
                shared_.insert({o, oid});
                subjects_.erase(o);
            } else {
                auto ret = objects_.insert({o, temp_node_id});

                if (ret.second) {
                    oid = temp_node_id;
                    temp_node_id++;
                } else {
                    oid = ret.first->second;
                }
            }
        } else {
            oid = shared_it->second;
        }

        predicates_.insert({p, predicates_.size() + 1});

        ++triplet_cnt_;

        if (triplet_cnt_ % 100000 == 0) {
            std::cout << triplet_cnt_ << " triples processed\r" << std::flush;
        }
    }
    std::cout << triplet_cnt_ << std::endl;
    malloc_trim(0);

    fin.close();
}

void DictionaryBuilder::ReassignIDAndSave(hash_map<std::string, uint>& map,
                                          std::ofstream& dict_out,
                                          std::string hashmap_path,
                                          uint menagement_file_offset,
                                          uint max_threads) {
    // hash -> (id, p_str)
    phmap::btree_map<std::size_t, std::pair<uint, const std::string*>> hash2id;
    phmap::flat_hash_map<uint, std::vector<std::string>> conflicts;
    uint nodes_per_thread = (map.size()) ? map.size() / max_threads : 0;
    std::size_t offset = 0;
    std::size_t size = 0;
    uint id = 1;
    uint cnt = 0;
    for (auto it = map.begin(); it != map.end(); it++) {
        it->second = id;
        size = it->first.size() + 1;
        dict_out.write((it->first + "\n").c_str(), size);

        if (nodes_per_thread && (id - 1) % nodes_per_thread == 0 && cnt < max_threads) {
            menagement_data_[menagement_file_offset++] = id;
            menagement_data_[menagement_file_offset++] = offset;
            cnt++;
        }
        id++;
        offset += size;

        std::size_t hash = std::hash<std::string>{}(it->first);
        auto ret = hash2id.insert({hash, {it->second, &it->first}});
        if (!ret.second) {
            if (ret.first->second.first != 0) {
                std::vector<std::string> c = {*ret.first->second.second, it->first};
                conflicts.insert({hash, c}).second;
                ret.first->second.first = 0;
            } else
                conflicts[hash].push_back(it->first);
        }
    }
    uint file_size = (sizeof(std::size_t) + 4) * hash2id.size();
    MMap<std::size_t> hashes = MMap<std::size_t>(hashmap_path, file_size);
    for (auto it = hash2id.begin(); it != hash2id.end(); it++)
        hashes.Write(it->first);
    hashes.CloseMap();

    MMap<uint> ids = MMap<uint>(hashmap_path, file_size);
    ids.offset_ = hash2id.size() * 2;
    for (auto it = hash2id.begin(); it != hash2id.end(); it++)
        ids.Write(it->second.first);
    ids.CloseMap();

    phmap::btree_map<std::size_t, std::pair<uint, const std::string*>>().swap(hash2id);

    if (conflicts.size() != 0) {
        std::cout << "conflict" << std::endl;
    }
}

void DictionaryBuilder::SaveDict(uint max_threads) {
    std::cout << "saving dictionary" << std::endl;

    std::ofstream predicate_out =
        std::ofstream(dict_path_ + "/predicates", std::ofstream::out | std::ofstream::binary);
    std::ofstream subject_out =
        std::ofstream(dict_path_ + "/subjects/nodes", std::ofstream::out | std::ofstream::binary);
    std::ofstream object_out =
        std::ofstream(dict_path_ + "/objects/nodes", std::ofstream::out | std::ofstream::binary);
    std::ofstream shared_out =
        std::ofstream(dict_path_ + "/shared/nodes", std::ofstream::out | std::ofstream::binary);
    subject_out.tie(nullptr);
    object_out.tie(nullptr);
    shared_out.tie(nullptr);

    std::thread t1([&]() {
        ReassignIDAndSave(subjects_, subject_out, dict_path_ + "/subjects/hash2id", 5, max_threads);
    });
    std::thread t2([&]() {
        ReassignIDAndSave(objects_, object_out, dict_path_ + "/objects/hash2id", 5 + max_threads * 2,
                          max_threads);
    });
    std::thread t3([&]() {
        ReassignIDAndSave(shared_, shared_out, dict_path_ + "/shared/hash2id", 5 + max_threads * 2 * 2,
                          max_threads);
    });
    t1.join();
    t2.join();
    t3.join();

    std::vector<const std::string*> predicates(predicates_.size() + 1);
    for (auto& p_pair : predicates_)
        predicates[p_pair.second] = &p_pair.first;
    for (uint pid = 1; pid <= predicates_.size(); pid++)
        predicate_out.write((*predicates[pid] + "\n").c_str(),
                            static_cast<long>(predicates[pid]->size() + 1));

    subject_out.close();
    object_out.close();
    shared_out.close();
    predicate_out.close();
}

void DictionaryBuilder::Build() {
    if (file_path_.empty())
        return;

    uint max_threads = 6;

    menagement_data_ = MMap<uint>(dict_path_ + "/menagement_data", (5 + (2 * max_threads * 3)) * 4);

    Init();

    auto beg = std::chrono::high_resolution_clock::now();
    BuildDict();
    auto end = std::chrono::high_resolution_clock::now();
    std::cout << "assign id takes " << std::chrono::duration<double, std::milli>(end - beg).count() << " ms."
              << std::endl;

    menagement_data_[0] = subjects_.size();
    menagement_data_[1] = predicates_.size();
    menagement_data_[2] = objects_.size();
    menagement_data_[3] = shared_.size();
    menagement_data_[4] = triplet_cnt_;

    beg = std::chrono::high_resolution_clock::now();
    SaveDict(max_threads);
    end = std::chrono::high_resolution_clock::now();
    std::cout << "save dictionary takes " << std::chrono::duration<double, std::milli>(end - beg).count()
              << " ms." << std::endl;

    menagement_data_.CloseMap();
}

void DictionaryBuilder::EncodeRDF(hash_map<uint, std::vector<std::pair<uint, uint>>>& pso) {
    std::cout << "encoding rdf." << std::endl;

    uint sid, pid, oid;
    std::string s, p, o;
    std::ifstream fin(file_path_, std::ios::in);

    uint triplet_cnt = 0;
    while (fin >> s >> p) {
        fin.ignore();
        std::getline(fin, o);
        for (o.pop_back(); o.back() == ' ' || o.back() == '.'; o.pop_back()) {
        }

        auto it = subjects_.find(s);
        sid = (it != subjects_.end()) ? shared_.size() + it->second : shared_.find(s)->second;
        it = objects_.find(o);
        oid =
            (it != objects_.end()) ? shared_.size() + subjects_.size() + it->second : shared_.find(o)->second;

        pid = predicates_.find(p)->second;

        pso[pid].push_back({sid, oid});

        triplet_cnt++;
        if (triplet_cnt % 100000 == 0) {
            std::cout << triplet_cnt << " triples processed\r" << std::flush;
        }
    }
    std::cout << triplet_cnt << std::endl;

    fin.close();
}

void DictionaryBuilder::Close() {
    std::thread t1([&]() { hash_map<std::string, uint>().swap(subjects_); });
    std::thread t2([&]() { hash_map<std::string, uint>().swap(objects_); });
    std::thread t3([&]() {
        hash_map<std::string, uint>().swap(shared_);
        hash_map<std::string, uint>().swap(predicates_);
    });

    t1.join();
    t2.join();
    t3.join();
}