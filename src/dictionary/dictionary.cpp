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

    std::cout << subject_cnt_ + object_cnt_ + shared_cnt_ << std::endl;
    std::cout << predicate_cnt_ << std::endl;

    id2predicate_ = std::vector<std::string>(predicate_cnt_ + 1);
    LoadPredicate(id2predicate_, predicate2id_);

    auto process_id2entity = [&](ulong type, std::string file_name,
                                 std::variant<Node<uint>, Node<ulong>>& id2entity) {
        if (type == 32)
            id2entity = Node<uint>(dict_path_ + file_name);
        else
            id2entity = Node<ulong>(dict_path_ + file_name);
    };

    auto build_cache = [&](uint start, uint end) {
        for (uint id = start; id <= end; id += 10) {
            if (id <= shared_cnt())
                delete[] ID2String(id, SPARQLParser::Term::Positon::kShared);
            else if (id <= shared_cnt() + subject_cnt())
                delete[] ID2String(id, SPARQLParser::Term::Positon::kSubject);
            else
                delete[] ID2String(id, SPARQLParser::Term::Positon::kObject);
        }
    };

    std::thread t1([&]() { process_id2entity(menagement_data[4], "/subjects/", id2subject_); });
    std::thread t2([&]() { process_id2entity(menagement_data[5], "/objects/", id2object_); });
    std::thread t3([&]() { process_id2entity(menagement_data[6], "/shared/", id2shared_); });
    t1.join();
    t2.join();
    t3.join();

    uint cpu_count = std::thread::hardware_concurrency();
    uint batch_size = (shared_cnt() + subject_cnt() + object_cnt()) / cpu_count;
    std::vector<std::thread> threads;
    for (uint i = 0; i < cpu_count; i++) {
        uint start = i * batch_size + 1;
        uint end = (i + 1) * batch_size;
        threads.emplace_back(std::thread([start, end, &build_cache]() { build_cache(start, end); }));
    }
    for (auto& t : threads)
        t.join();

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

const char* Dictionary::ID2String(uint id, SPARQLParser::Term::Positon pos) {
    if (pos == SPARQLParser::Term::Positon::kPredicate) {
        return id2predicate_[id].c_str();
    }

    if (id <= shared_cnt_) {
        return (std::holds_alternative<Node<uint>>(id2shared_)) ? std::get<Node<uint>>(id2shared_)[id]
                                                                : std::get<Node<ulong>>(id2shared_)[id];
    }

    switch (pos) {
        case SPARQLParser::Term::Positon::kSubject:
            return (std::holds_alternative<Node<uint>>(id2subject_))
                       ? std::get<Node<uint>>(id2subject_)[id - shared_cnt_]
                       : std::get<Node<ulong>>(id2subject_)[id - shared_cnt_];
        case SPARQLParser::Term::Positon::kObject:
            return (std::holds_alternative<Node<uint>>(id2object_))
                       ? std::get<Node<uint>>(id2object_)[id - shared_cnt_ - subject_cnt_]
                       : std::get<Node<ulong>>(id2object_)[id - shared_cnt_ - subject_cnt_];
        default:
            break;
    }
    throw std::runtime_error("Unhandled case in ID2String");
}

uint Dictionary::String2ID(const std::string& str, SPARQLParser::Term::Positon pos) {
    switch (pos) {
        case SPARQLParser::Term::Positon::kSubject:  // subject
            return FindInMaps(subject_cnt_, kSubjectMap, str);
        case SPARQLParser::Term::Positon::kPredicate: {  // predicate
            return Find(kPredicateMap, str);
        }
        case SPARQLParser::Term::Positon::kObject:  // object
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