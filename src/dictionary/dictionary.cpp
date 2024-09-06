#include "rdf-tdaa/dictionary/dictionary.hpp"

Dictionary::Loader::Loader(std::string dict_path, uint subject_cnt, uint object_cnt, uint shared_cnt)
    : dict_path_(dict_path), subject_cnt_(subject_cnt), object_cnt_(object_cnt), shared_cnt_(shared_cnt) {}

void Dictionary::Loader::SubLoadID2Node(std::vector<std::string>& id2node,
                                        MMap<char>& node_file,
                                        uint start_id,
                                        ulong start_offset,
                                        ulong end_offset) {
    uint id = start_id;
    std::string node;
    ulong i = start_offset;
    for (; i < end_offset; i++) {
        char& c = node_file[i];
        if (c != '\n') {
            node.push_back(c);
        } else {
            id2node[id] = node;
            id++;
            node.clear();
        }
    }
}

bool Dictionary::Loader::LoadPredicate(std::vector<std::string>& id2predicate,
                                       hash_map<std::string, uint>& predicate2id) {
    std::ifstream predicate_in(dict_path_ + "/predicates", std::ofstream::out | std::ofstream::binary);
    std::string predicate;
    uint id = 1;
    while (std::getline(predicate_in, predicate)) {
        predicate2id[predicate] = id;
        id2predicate[id] = predicate;
        id++;
    }
    predicate_in.close();
    return true;
}

void Dictionary::Loader::LoadID2Node(std::vector<std::string>& id2subject_,
                                     std::vector<std::string>& id2object_,
                                     std::vector<std::string>& id2shared_) {
    MMap<char> subject_file = MMap<char>(dict_path_ + "/subjects/nodes");
    MMap<char> object_file = MMap<char>(dict_path_ + "/objects/nodes");
    MMap<char> shared_file = MMap<char>(dict_path_ + "/shared/nodes");
    MMap<ulong> menagement_data = MMap<ulong>(dict_path_ + "/menagement_data");

    std::array<std::pair<std::vector<std::string>*, MMap<char>*>, 3> pos;
    pos[0] = {&id2subject_, &subject_file};
    pos[1] = {&id2object_, &object_file};
    pos[2] = {&id2shared_, &shared_file};

    uint max_threads = 6;

    std::vector<std::thread> threads;

    for (uint i = 0; i < pos.size(); i++) {
        ulong base = 5 + max_threads * 2 * i;

        for (uint t = 0; t < max_threads; t++) {
            ulong start_id = menagement_data[base++];
            ulong start_offset = menagement_data[base++];
            ulong end_offset = (t != max_threads - 1) ? menagement_data[base + 1] : pos[i].second->size_;

            threads.emplace_back(std::bind(&Dictionary::Loader::SubLoadID2Node, this, std::ref(*pos[i].first),
                                           std::ref(*pos[i].second), start_id, start_offset, end_offset));
        }

        for (auto& thread : threads)
            thread.join();
        threads.clear();
    }

    subject_file.CloseMap();
    object_file.CloseMap();
    shared_file.CloseMap();
    menagement_data.CloseMap();
}

Dictionary::Dictionary() {}

Dictionary::Dictionary(std::string& dict_path_) : dict_path_(dict_path_) {
    InitLoad();
}

void Dictionary::InitLoad() {
    MMap<ulong> menagement_data = MMap<ulong>(dict_path_ + "/menagement_data");

    subject_cnt_ = menagement_data[0];
    id2subject_ = std::vector<std::string>(subject_cnt_ + 1);

    predicate_cnt_ = menagement_data[1];
    id2predicate_ = std::vector<std::string>(predicate_cnt_ + 1);

    object_cnt_ = menagement_data[2];
    id2object_ = std::vector<std::string>(object_cnt_ + 1);

    shared_cnt_ = menagement_data[3];
    id2shared_ = std::vector<std::string>(shared_cnt_ + 1);

    triplet_cnt_ = menagement_data[4];

    menagement_data.CloseMap();

    std::string file_path = dict_path_ + "/subjects/hash2id";
    subject_hashes_ = MMap<std::size_t>(file_path);
    subject_ids_ = MMap<uint>(file_path);
    file_path = dict_path_ + "/objects/hash2id";
    object_hashes_ = MMap<std::size_t>(file_path);
    object_ids_ = MMap<uint>(file_path);
    file_path = dict_path_ + "/shared/hash2id";
    shared_hashes_ = MMap<std::size_t>(file_path);
    shared_ids_ = MMap<uint>(file_path);
}

long Dictionary::binarySearch(MMap<std::size_t> arr, long length, std::size_t target) {
    long left = 0;
    long right = length - 1;

    while (left <= right) {
        long mid = left + (right - left) / 2;

        if (arr[mid] == target)
            return mid;
        else if (arr[mid] < target)
            left = mid + 1;
        else
            right = mid - 1;
    }

    return -1;
}

uint Dictionary::Find(Map map, const std::string& str) {
    hash_map<std::string, uint>::iterator it;
    if (map == Map::kPredicateMap) {
        it = predicate2id_.find(str);
        return (it != predicate2id_.end()) ? it->second : 0;
    }

    std::size_t hash = std::hash<std::string>{}(str);
    long pos;
    uint id = 0;
    if (map == Map::kSubjectMap) {
        pos = binarySearch(subject_hashes_, subject_cnt_, hash);
        id = (pos != -1) ? subject_ids_[subject_cnt_ * 2 + pos] : 0;
    }
    if (map == Map::kObjectMap) {
        pos = binarySearch(object_hashes_, object_cnt_, hash);
        id = (pos != -1) ? object_ids_[object_cnt_ * 2 + pos] : 0;
    }
    if (map == Map::kSharedMap) {
        pos = binarySearch(shared_hashes_, shared_cnt_, hash);
        id = (pos != -1) ? shared_ids_[shared_cnt_ * 2 + pos] : 0;
    }
    return id;
}

uint Dictionary::FindInMaps(uint cnt, Map map, const std::string& str) {
    uint ret;
    if (shared_cnt_ > cnt) {
        ret = Find(kSharedMap, str);
        if (ret)
            return ret;
        ret = Find(map, str);
        if (ret)
            return (map == Map::kSubjectMap) ? shared_cnt_ + ret : shared_cnt_ + subject_cnt_ + ret;
    } else {
        ret = Find(map, str);
        if (ret)
            return (map == Map::kSubjectMap) ? shared_cnt_ + ret : shared_cnt_ + subject_cnt_ + ret;
        ret = Find(kSharedMap, str);
        if (ret)
            return ret;
    }
    return 0;
}

void Dictionary::Close() {
    std::thread t1([&]() {
        std::vector<std::string>().swap(id2predicate_);
        std::vector<std::string>().swap(id2subject_);
        std::vector<std::string>().swap(id2object_);
        std::vector<std::string>().swap(id2shared_);
    });

    std::thread t2([&]() {
        hash_map<std::string, uint>().swap(predicate2id_);
        subject_hashes_.CloseMap();
        object_hashes_.CloseMap();
        shared_hashes_.CloseMap();
        subject_ids_.CloseMap();
        object_ids_.CloseMap();
        shared_ids_.CloseMap();
    });

    t1.join();
    t2.join();
}

void Dictionary::Load() {
    std::ios::sync_with_stdio(false);

    Loader loader(dict_path_, subject_cnt_, object_cnt_, shared_cnt_);

    loader.LoadPredicate(id2predicate_, predicate2id_);
    loader.LoadID2Node(id2subject_, id2object_, id2shared_);
}

std::string& Dictionary::ID2String(uint id, Pos pos) {
    if (pos == kPredicate) {
        return id2predicate_[id];
    }

    if (id <= shared_cnt_) {
        return id2shared_[id];
    }

    switch (pos) {
        case kSubject:
            return id2subject_[id - shared_cnt_];
        case kObject:
            return id2object_[id - shared_cnt_ - subject_cnt_];
        default:
            break;
    }
    throw std::runtime_error("Unhandled case in ID2String");
}

uint Dictionary::String2ID(const std::string& str, Pos pos) {
    switch (pos) {
        case kSubject:  // subject
            return FindInMaps(subject_cnt_, kSubjectMap, str);
        case kPredicate: {  // predicate
            return Find(kPredicateMap, str);
        }
        case kObject:  // object
            return FindInMaps(object_cnt_, kObjectMap, str);
        default:
            break;
    }
    return 0;
}

uint Dictionary::subject_cnt() {
    return subject_cnt_;
}

uint Dictionary::predicate_cnt() {
    return predicate_cnt_;
}

uint Dictionary::object_cnt() {
    return object_cnt_;
}

uint Dictionary::shared_cnt() {
    return shared_cnt_;
}

uint Dictionary::triplet_cnt() {
    return triplet_cnt_;
}

uint Dictionary::max_id() {
    return shared_cnt_ + subject_cnt_ + object_cnt_;
};