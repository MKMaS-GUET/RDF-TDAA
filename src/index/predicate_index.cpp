#include "rdf-tdaa/index/predicate_index.hpp"
#include <iostream>
#include "streamvbyte.h"

void PredicateIndex::Index::Build(std::vector<std::pair<uint, uint>>& so_pairs) {
    for (const auto& so : so_pairs) {
        s_set.push_back(so.first);
        o_set.push_back(so.second);
    }
    std::sort(s_set.begin(), s_set.end());
    s_set.erase(std::unique(s_set.begin(), s_set.end()), s_set.end());

    std::sort(o_set.begin(), o_set.end());
    o_set.erase(std::unique(o_set.begin(), o_set.end()), o_set.end());
}

void PredicateIndex::Index::BuildMap() {
    for (uint i = 0; i < s_set.size(); i++)
        sid2offset[s_set[i]] = i;

    for (uint i = 0; i < o_set.size(); i++)
        oid2offset[o_set[i]] = i;

    std::vector<uint>().swap(s_set);
    std::vector<uint>().swap(o_set);
}

void PredicateIndex::Index::Clear() {
    std::vector<uint>().swap(s_set);
    std::vector<uint>().swap(o_set);

    phmap::flat_hash_map<uint, uint>().swap(sid2offset);
    phmap::flat_hash_map<uint, uint>().swap(oid2offset);
}

PredicateIndex::PredicateIndex() {}

PredicateIndex::PredicateIndex(std::string file_path, uint max_predicate_id)
    : file_path_(file_path), max_predicate_id_(max_predicate_id) {
    predicate_index_mmap_ = MMap<uint>(file_path_ + "predicate_index");

    std::string index_path = file_path_ + "predicate_index_arrays";
    if (compress_predicate_index_)
        predicate_index_arrays_ = MMap<uint8_t>(index_path);
    else
        predicate_index_arrays_no_compress_ = MMap<uint>(index_path);

    ps_sets_ = std::vector<std::span<uint>>(max_predicate_id_);
    po_sets_ = std::vector<std::span<uint>>(max_predicate_id_);

    phmap::btree_map<uint, uint> s_sizes, o_sizes;
    for (uint pid = 1; pid <= max_predicate_id_; pid++) {
        s_sizes.insert({GetSSetSize(pid), pid});
        o_sizes.insert({GetOSetSize(pid), pid});
    }
    uint cnt = 0;
    for (auto rit = s_sizes.rbegin(); rit != s_sizes.rend() && cnt < 5; ++rit, cnt++)
        GetSSet(rit->second);
    cnt = 0;
    for (auto rit = o_sizes.rbegin(); rit != o_sizes.rend() && cnt < 5; ++rit, cnt++)
        GetOSet(rit->second);
}

PredicateIndex::PredicateIndex(
    std::shared_ptr<phmap::flat_hash_map<uint, std::vector<std::pair<uint, uint>>>> pso,
    std::string file_path,
    uint max_predicate_id)
    : file_path_(file_path), pso_(pso), max_predicate_id_(max_predicate_id) {}

void PredicateIndex::BuildPredicateIndex() {
    std::vector<std::pair<uint, uint>> predicate_rank;
    for (uint pid = 1; pid <= max_predicate_id_; pid++) {
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

    for (uint i = 0; i < max_predicate_id_; i++)
        task_queue.push_back(predicate_rank[i].first);

    std::vector<std::thread> threads;
    for (uint tid = 0; tid < cpu_count; tid++) {
        threads.emplace_back(std::bind(&PredicateIndex::SubBuildPredicateIndex, this, &task_queue,
                                       &task_queue_mutex, &task_queue_cv, &task_queue_empty));
    }
    for (auto& t : threads)
        t.join();
}

void PredicateIndex::SubBuildPredicateIndex(std::deque<uint>* task_queue,
                                            std::mutex* task_queue_mutex,
                                            std::condition_variable* task_queue_cv,
                                            std::atomic<bool>* task_queue_empty) {
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

        index_[pid - 1].Build(pso_->at(pid));
    }
}

void PredicateIndex::StorePredicateIndexNoCompress() {
    auto beg = std::chrono::high_resolution_clock::now();

    ulong predicate_index_arrays_file_size = 0;
    for (uint pid = 1; pid <= max_predicate_id_; pid++) {
        predicate_index_arrays_file_size += ulong(index_[pid - 1].s_set.size() * 4ul);
        predicate_index_arrays_file_size += ulong(index_[pid - 1].o_set.size() * 4ul);
    }
    MMap<uint> predicate_index_mmap = MMap<uint>(file_path_ + "predicate_index", max_predicate_id_ * 2 * 4);
    MMap<uint> predicate_index_arrays =
        MMap<uint>(file_path_ + "predicate_index_arrays", predicate_index_arrays_file_size);

    ulong arrays_file_offset = 0;

    std::vector<uint>* ps_set;
    std::vector<uint>* po_set;
    for (uint pid = 1; pid <= max_predicate_id_; pid++) {
        ps_set = &index_[pid - 1].s_set;
        po_set = &index_[pid - 1].o_set;

        predicate_index_mmap[(pid - 1) * 2] = arrays_file_offset;
        for (auto it = ps_set->begin(); it != ps_set->end(); it++) {
            predicate_index_arrays[arrays_file_offset] = *it;
            arrays_file_offset++;
        }

        predicate_index_mmap[(pid - 1) * 2 + 1] = arrays_file_offset;
        for (auto it = po_set->begin(); it != po_set->end(); it++) {
            predicate_index_arrays[arrays_file_offset] = *it;
            arrays_file_offset++;
        }

        index_[pid - 1].BuildMap();
    }

    predicate_index_mmap.CloseMap();
    predicate_index_arrays.CloseMap();

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> diff = end - beg;
    std::cout << "store predicate index takes " << diff.count() << " ms.               " << std::endl;
}

void PredicateIndex::StorePredicateIndex() {
    ulong predicate_index_file_size_ = max_predicate_id_ * 4 * 4;

    MMap<uint> predicate_index = MMap<uint>(file_path_ + "predicate_index", predicate_index_file_size_);

    ulong total_compressed_size = 0;
    std::vector<std::pair<uint8_t*, uint>> compressed_ps_set(max_predicate_id_);
    std::vector<std::pair<uint8_t*, uint>> compressed_po_set(max_predicate_id_);

    std::vector<uint>* ps_set;
    std::vector<uint>* po_set;
    uint* set_buffer;
    uint buffer_offset;
    uint8_t* compressed_buffer;
    ulong compressed_size;
    uint last;
    for (uint pid = 1; pid <= max_predicate_id_; pid++) {
        ps_set = &index_[pid - 1].s_set;
        po_set = &index_[pid - 1].o_set;

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

        index_[pid - 1].BuildMap();
    }

    MMap<uint8_t> predicate_index_arrays =
        MMap<uint8_t>(file_path_ + "predicate_index_arrays", total_compressed_size);
    for (uint pid = 1; pid <= max_predicate_id_; pid++) {
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

void PredicateIndex::Build() {
    index_ = std::vector<Index>(max_predicate_id_);
    BuildPredicateIndex();
}

void PredicateIndex::Store() {
    if (compress_predicate_index_)
        StorePredicateIndex();
    else
        StorePredicateIndexNoCompress();
}

std::span<uint>& PredicateIndex::GetSSet(uint pid) {
    if (ps_sets_[pid - 1].size() == 0) {
        if (compress_predicate_index_) {
            uint s_array_offset = predicate_index_mmap_[(pid - 1) * 4];
            uint s_compressed_size = predicate_index_mmap_[(pid - 1) * 4 + 2] - s_array_offset;

            uint8_t* compressed_buffer = new uint8_t[s_compressed_size];
            for (uint i = 0; i < s_compressed_size; i++)
                compressed_buffer[i] = predicate_index_arrays_[s_array_offset + i];

            uint reco_size = predicate_index_mmap_[(pid - 1) * 4 + 1];
            uint* recovdata = new uint[reco_size];

            streamvbyte_decode(compressed_buffer, recovdata, reco_size);

            for (uint i = 1; i < reco_size; i++)
                recovdata[i] += recovdata[i - 1];

            ps_sets_[pid - 1] = std::span<uint>(recovdata, reco_size);

        } else {
            uint s_array_offset = predicate_index_mmap_[(pid - 1) * 2];
            uint s_array_size = predicate_index_mmap_[(pid - 1) * 2 + 1] - s_array_offset;

            uint* set = new uint[s_array_size];
            for (uint i = 0; i < s_array_size; i++)
                set[i] = predicate_index_arrays_no_compress_[s_array_offset + i];

            ps_sets_[pid - 1] = std::span<uint>(set, s_array_size);
        }
    }
    return ps_sets_[pid - 1];
};

std::span<uint>& PredicateIndex::GetOSet(uint pid) {
    if (po_sets_[pid - 1].size() == 0) {
        if (compress_predicate_index_) {
            uint o_array_offset = predicate_index_mmap_[(pid - 1) * 4 + 2];
            uint o_compressed_size;
            if (pid != max_predicate_id_)
                o_compressed_size = predicate_index_mmap_[pid * 4] - o_array_offset;
            else
                o_compressed_size = predicate_index_arrays_.size_ - o_array_offset;

            uint8_t* compressed_buffer = new uint8_t[o_compressed_size];
            for (uint i = 0; i < o_compressed_size; i++)
                compressed_buffer[i] = predicate_index_arrays_[o_array_offset + i];

            uint reco_size = predicate_index_mmap_[(pid - 1) * 4 + 3];
            uint* recovdata = new uint[reco_size];
            streamvbyte_decode(compressed_buffer, recovdata, reco_size);

            for (uint i = 1; i < reco_size; i++)
                recovdata[i] += recovdata[i - 1];

            po_sets_[pid - 1] = std::span<uint>(recovdata, reco_size);
        } else {
            uint o_array_offset = predicate_index_mmap_[(pid - 1) * 2 + 1];
            uint o_array_size;
            if (pid != max_predicate_id_)
                o_array_size = predicate_index_mmap_[pid * 2] - o_array_offset;
            else
                o_array_size = predicate_index_mmap_.size_ / 4 - o_array_offset;

            uint* set = new uint[o_array_size];
            for (uint i = 0; i < o_array_size; i++)
                set[i] = predicate_index_arrays_no_compress_[o_array_offset + i];

            po_sets_[pid - 1] = std::span<uint>(set, set + o_array_size);
        }
    }
    return po_sets_[pid - 1];
}

uint PredicateIndex::GetSSetSize(uint pid) {
    if (ps_sets_[pid - 1].size() == 0) {
        uint s_array_size;
        if (compress_predicate_index_) {
            s_array_size = predicate_index_mmap_[(pid - 1) * 4 + 1];
        } else {
            uint s_array_offset = predicate_index_mmap_[(pid - 1) * 2];
            s_array_size = predicate_index_mmap_[(pid - 1) * 2 + 1] - s_array_offset;
        }
        return s_array_size;
    }
    return ps_sets_[pid - 1].size();
}

uint PredicateIndex::GetOSetSize(uint pid) {
    if (po_sets_[pid - 1].size() == 0) {
        uint o_array_size;
        if (compress_predicate_index_) {
            o_array_size = predicate_index_mmap_[(pid - 1) * 4 + 3];
        } else {
            uint o_array_offset = predicate_index_mmap_[(pid - 1) * 2 + 1];
            if (pid != max_predicate_id_)
                o_array_size = predicate_index_mmap_[pid * 2] - o_array_offset;
            else
                o_array_size = predicate_index_mmap_.size_ / 4 - o_array_offset;
        }
        return o_array_size;
    }
    return po_sets_[pid - 1].size();
}

void PredicateIndex::Close() {
    predicate_index_mmap_.CloseMap();
    if (compress_predicate_index_)
        predicate_index_arrays_.CloseMap();
    else
        predicate_index_arrays_no_compress_.CloseMap();

    std::vector<std::span<uint>>().swap(ps_sets_);
    std::vector<std::span<uint>>().swap(po_sets_);
}