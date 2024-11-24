#include "rdf-tdaa/utils/join_list.hpp"

JoinList::JoinList() {
    lists_ = std::vector<std::span<uint>>();
}

JoinList::JoinList(std::vector<std::span<uint>>& lists) {
    AddLists(lists);
}

void JoinList::AddList(const std::span<uint>& list) {
    if (lists_.size() == 0 || list.size() == 0) {
        lists_.push_back(list);
        return;
    }
    uint first_val = list.operator[](0);
    for (long unsigned int i = 0; i < lists_.size(); i++) {
        if (lists_[i].size() > 0)
            if (lists_[i][0] > first_val) {
                lists_.insert(lists_.begin() + i, list);
                return;
            }
    }

    lists_.push_back(list);
}

void JoinList::AddLists(const std::vector<std::span<uint>>& lists) {
    for (auto it = lists.begin(); it != lists.end(); it++) {
        AddList(*it);
    }
}

std::span<uint> JoinList::Shortest() {
    long unsigned int min_i = 0;
    long unsigned int min = lists_[0].size();
    for (long unsigned int i = 0; i < lists_.size(); i++) {
        if (lists_[i].size() < min) {
            min = lists_[i].size();
            min_i = i;
        }
    }
    return lists_[min_i];
}

void JoinList::UpdateCurrentPostion() {
    for (long unsigned int i = 0; i < lists_.size(); i++) {
        list_current_pos_.push_back(lists_[i].begin());
    }
}

void JoinList::Seek(int i, uint val) {
    std::span<uint>& p_r = lists_[i];

    auto it = list_current_pos_[i];
    auto end = p_r.end();
    for (; it < end; it = it + 2) {
        if (*it >= val) {
            if (*(it - 1) >= val) {
                list_current_pos_[i] = it - 1;
                return;
            }
            list_current_pos_[i] = it;
            return;
        }
    }
    if (it == end) {
        if (*(it - 1) >= val) {
            list_current_pos_[i] = it - 1;
            return;
        }
    }
    list_current_pos_[i] = end;
}

uint JoinList::GetCurrentValOfList(int i) {
    return *list_current_pos_[i];
}

void JoinList::NextVal(int i) {
    list_current_pos_[i]++;
}

std::span<uint> JoinList::GetShortest() {
    uint idx = 0;
    for (long unsigned int i = 0; i < lists_.size(); i++) {
        if (lists_[i].size() < lists_[idx].size())
            idx = i;
    }
    return lists_[idx];
}

std::span<uint> JoinList::GetListByIndex(int i) {
    return lists_[i];
}

bool JoinList::HasEmpty() {
    for (long unsigned int i = 0; i < lists_.size(); i++) {
        if (lists_[i].size() == 0)
            return true;
    }
    return false;
}

bool JoinList::AtEnd(int i) {
    std::span<uint> p_r = lists_[i];
    return list_current_pos_[i] == p_r.end();
}

void JoinList::Clear() {
    lists_.clear();
    list_current_pos_.clear();
}

int JoinList::Size() {
    return lists_.size();
}
