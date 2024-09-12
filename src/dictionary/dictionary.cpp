#include "rdf-tdaa/dictionary/dictionary.hpp"
#include "rdf-tdaa/utils/vbyte.hpp"

bool Dictionary::LoadPredicate(std::vector<std::string>& id2predicate,
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

Dictionary::Dictionary() {}

Dictionary::Dictionary(std::string& dict_path) : dict_path_(dict_path) {
    std::string file_path = dict_path_ + "/subjects/hash2id";
    subject_hashes_ = MMap<std::size_t>(file_path);
    subject_ids_ = MMap<uint>(file_path);
    file_path = dict_path_ + "/objects/hash2id";
    object_hashes_ = MMap<std::size_t>(file_path);
    object_ids_ = MMap<uint>(file_path);
    file_path = dict_path_ + "/shared/hash2id";
    shared_hashes_ = MMap<std::size_t>(file_path);
    shared_ids_ = MMap<uint>(file_path);

    MMap<ulong> menagement_data = MMap<ulong>(dict_path_ + "/menagement_data");

    subject_cnt_ = menagement_data[0];
    predicate_cnt_ = menagement_data[1];
    object_cnt_ = menagement_data[2];
    shared_cnt_ = menagement_data[3];

    id2predicate_ = std::vector<std::string>(predicate_cnt_ + 1);
    LoadPredicate(id2predicate_, predicate2id_);

    std::thread t1([&]() {
        if (menagement_data[4] == 32)
            id2subject_ = Node<uint>(dict_path_ + "/subjects/");
        else
            id2subject_ = Node<ulong>(dict_path_ + "/subjects/");
    });
    std::thread t2([&]() {
        if (menagement_data[5] == 32)
            id2object_ = Node<uint>(dict_path_ + "/objects/");
        else
            id2object_ = Node<ulong>(dict_path_ + "/objects/");
    });
    std::thread t3([&]() {
        if (menagement_data[6] == 32)
            id2shared_ = Node<uint>(dict_path_ + "/shared/");
        else
            id2shared_ = Node<ulong>(dict_path_ + "/shared/");
    });

    t1.join();
    t2.join();
    t3.join();
    menagement_data.CloseMap();
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
    hash_map<std::string, uint>().swap(predicate2id_);
    subject_hashes_.CloseMap();
    object_hashes_.CloseMap();
    shared_hashes_.CloseMap();
    subject_ids_.CloseMap();
    object_ids_.CloseMap();
    shared_ids_.CloseMap();
}

const char* Dictionary::ID2String(uint id, Pos pos) {
    if (pos == kPredicate) {
        return id2predicate_[id].c_str();
    }

    if (id <= shared_cnt_) {
        return (std::holds_alternative<Node<uint>>(id2shared_)) ? std::get<Node<uint>>(id2shared_)[id]
                                                                : std::get<Node<ulong>>(id2shared_)[id];
    }

    switch (pos) {
        case kSubject:
            return (std::holds_alternative<Node<uint>>(id2subject_))
                       ? std::get<Node<uint>>(id2subject_)[id - shared_cnt_]
                       : std::get<Node<ulong>>(id2subject_)[id - shared_cnt_];
        case kObject:
            return (std::holds_alternative<Node<uint>>(id2object_))
                       ? std::get<Node<uint>>(id2object_)[id - shared_cnt_ - subject_cnt_]
                       : std::get<Node<ulong>>(id2object_)[id - shared_cnt_ - subject_cnt_];
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

uint Dictionary::max_id() {
    return shared_cnt_ + subject_cnt_ + object_cnt_;
};