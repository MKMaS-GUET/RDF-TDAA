#include "rdf-tdaa/utils/join_list.hpp"

JoinList::JoinList() {
    lists_ = std::vector<std::shared_ptr<std::vector<uint>>>();
}

JoinList::JoinList(std::vector<std::shared_ptr<std::vector<uint>>>& lists) {
    AddVectors(lists);
}

void JoinList::AddVector(std::shared_ptr<std::vector<uint>>& list) {
    if (lists_.size() == 0) {
        lists_.push_back(list);
        return;
    }

    uint first_val = list->operator[](0);
    for (long unsigned int i = 0; i < lists_.size(); i++) {
        if (lists_[i]->operator[](0) > first_val) {
            lists_.insert(lists_.begin() + i, list);
            return;
        }
    }

    lists_.push_back(list);
}

void JoinList::AddVectors(std::vector<std::shared_ptr<std::vector<uint>>>& lists) {
    for (auto it = lists.begin(); it != lists.end(); it++) {
        AddVector(*it);
    }
}

std::shared_ptr<std::vector<uint>> JoinList::Shortest() {
    long unsigned int min_i = 0;
    long unsigned int min = lists_[0]->size();
    for (long unsigned int i = 0; i < lists_.size(); i++) {
        if (lists_[i]->size() < min) {
            min = lists_[i]->size();
            min_i = i;
        }
    }
    return lists_[min_i];
}

void JoinList::UpdateCurrentPostion() {
    for (long unsigned int i = 0; i < lists_.size(); i++) {
        vector_current_pos_.push_back(lists_[i]->begin());
    }
}

void JoinList::Seek(int i, uint val) {
    std::shared_ptr<std::vector<uint>> p_r = lists_[i];

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

uint JoinList::GetCurrentValOfRange(int i) {
    return *vector_current_pos_[i];
}

void JoinList::NextVal(int i) {
    vector_current_pos_[i]++;
}

std::shared_ptr<std::vector<uint>> JoinList::GetRangeByIndex(int i) {
    return lists_[i];
}

bool JoinList::HasEmpty() {
    for (long unsigned int i = 0; i < lists_.size(); i++) {
        if (lists_[i]->size() == 0) {
            return true;
        }
    }
    return false;
}

bool JoinList::AtEnd(int i) {
    std::shared_ptr<std::vector<uint>> p_r = lists_[i];
    return vector_current_pos_[i] == p_r->end();
}

void JoinList::Clear() {
    lists_.clear();
    vector_current_pos_.clear();
}

int JoinList::Size() {
    return lists_.size();
}
