#include "rdf-tdaa/utils/result_list.hpp"

ResultList::ResultList() {
    results_ = std::vector<std::shared_ptr<std::vector<uint>>>();
}

void ResultList::AddVector(std::shared_ptr<std::vector<uint>> range) {
    if (results_.size() == 0) {
        results_.push_back(range);
        return;
    }

    uint first_val = range->operator[](0);
    for (long unsigned int i = 0; i < results_.size(); i++) {
        if (results_[i]->operator[](0) > first_val) {
            results_.insert(results_.begin() + i, range);
            return;
        }
    }

    results_.push_back(range);
}

bool ResultList::AddVectors(std::vector<std::shared_ptr<std::vector<uint>>> ranges) {
    for (std::vector<std::shared_ptr<std::vector<uint>>>::iterator it = ranges.begin(); it != ranges.end();
         it++) {
        if ((*it)->size() == 0)
            return 0;
        AddVector(*it);
    }
    return 1;
}

std::shared_ptr<std::vector<uint>> ResultList::Shortest() {
    long unsigned int min_i = 0;
    long unsigned int min = results_[0]->size();
    for (long unsigned int i = 0; i < results_.size(); i++) {
        if (results_[i]->size() < min) {
            min = results_[i]->size();
            min_i = i;
        }
    }
    return results_[min_i];
}

void ResultList::UpdateCurrentPostion() {
    for (long unsigned int i = 0; i < results_.size(); i++) {
        vector_current_pos_.push_back(results_[i]->begin());
    }
}

void ResultList::Seek(int i, uint val) {
    std::shared_ptr<std::vector<uint>> p_r = results_[i];

    auto it = vector_current_pos_[i];
    auto end = p_r->end();
    for (; it < end; it = it + 2) {
        if (*it >= val) {
            if (*(it - 1) >= val) {
                vector_current_pos_[i] = it - 1;
                return;
            }
            vector_current_pos_[i] = it;
            return;
        }
    }
    if (it == end) {
        if (*(it - 1) >= val) {
            vector_current_pos_[i] = it - 1;
            return;
        }
    }
    vector_current_pos_[i] = end;
}

uint ResultList::GetCurrentValOfRange(int i) {
    return *vector_current_pos_[i];
}

void ResultList::NextVal(int i) {
    vector_current_pos_[i]++;
}

std::shared_ptr<std::vector<uint>> ResultList::GetRangeByIndex(int i) {
    return results_[i];
}

bool ResultList::HasEmpty() {
    for (long unsigned int i = 0; i < results_.size(); i++) {
        if (results_[i]->size() == 0) {
            return true;
        }
    }
    return false;
}

bool ResultList::AtEnd(int i) {
    std::shared_ptr<std::vector<uint>> p_r = results_[i];
    return vector_current_pos_[i] == p_r->end();
}

void ResultList::Clear() {
    results_.clear();
    vector_current_pos_.clear();
}

int ResultList::Size() {
    return results_.size();
}
