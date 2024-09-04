#include "rdf-tdaa/utils/result_list.hpp"

ResultList::Result::Iterator::Iterator() : ptr_(nullptr) {}

ResultList::Result::Iterator::Iterator(uint* p) : ptr_(p) {}

ResultList::Result::Iterator::Iterator(const Iterator& it) : ptr_(it.ptr_) {}

ResultList::Result::Iterator& ResultList::Result::Iterator::operator++() {
    ++ptr_;
    return *this;
}

ResultList::Result::Iterator ResultList::Result::Iterator::operator++(int) {
    Iterator tmp(*this);
    operator++();
    return tmp;
}

uint ResultList::Result::Iterator::operator-(Result::Iterator r_it) {
    return ptr_ - r_it.ptr_;
}

ResultList::Result::Iterator ResultList::Result::Iterator::operator-(int num) {
    return Iterator(ptr_ - num);
}

ResultList::Result::Iterator ResultList::Result::Iterator::operator+(int num) {
    return Iterator(ptr_ + num);
}

bool ResultList::Result::Iterator::operator==(const Iterator& rhs) const {
    return ptr_ == rhs.ptr_;
}

bool ResultList::Result::Iterator::operator!=(const Iterator& rhs) const {
    return ptr_ != rhs.ptr_;
}

bool ResultList::Result::Iterator::operator<(const Iterator& rhs) const {
    return ptr_ < rhs.ptr_;
}

uint& ResultList::Result::Iterator::operator*() {
    return *ptr_;
}

ResultList::Result::Result() : start_(nullptr), size_(0) {}

ResultList::Result::Result(uint* start, uint size) : start_(start), size_(size) {}

ResultList::Result::~Result() {
    delete[] start_;
    start_ = nullptr;
}

ResultList::Result::Iterator ResultList::Result::Begin() {
    return Iterator(start_);
}

ResultList::Result::Iterator ResultList::Result::End() {
    return Iterator(start_ + size_);
}

uint& ResultList::Result::operator[](uint i) {
    if (i >= 0 && i < size_) {
        return *(start_ + i);
    }
    return *start_;
}

uint ResultList::Result::Size() {
    return size_;
}

ResultList::ResultList() {
    results_ = std::vector<std::shared_ptr<Result>>();
}

void ResultList::AddVector(std::shared_ptr<Result> range) {
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

bool ResultList::AddVectors(std::vector<std::shared_ptr<Result>> ranges) {
    for (std::vector<std::shared_ptr<Result>>::iterator it = ranges.begin(); it != ranges.end(); it++) {
        if ((*it)->Size() == 0)
            return 0;
        AddVector(*it);
    }
    return 1;
}

std::shared_ptr<ResultList::Result> ResultList::Shortest() {
    long unsigned int min_i = 0;
    long unsigned int min = results_[0]->Size();
    for (long unsigned int i = 0; i < results_.size(); i++) {
        if (results_[i]->Size() < min) {
            min = results_[i]->Size();
            min_i = i;
        }
    }
    return results_[min_i];
}

void ResultList::UpdateCurrentPostion() {
    for (long unsigned int i = 0; i < results_.size(); i++) {
        vector_current_pos_.push_back(results_[i]->Begin());
    }
}

void ResultList::Seek(int i, uint val) {
    std::shared_ptr<Result> p_r = results_[i];

    auto it = vector_current_pos_[i];
    auto end = p_r->End();
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

std::shared_ptr<ResultList::Result> ResultList::GetRangeByIndex(int i) {
    return results_[i];
}

bool ResultList::HasEmpty() {
    for (long unsigned int i = 0; i < results_.size(); i++) {
        if (results_[i]->Size() == 0) {
            return true;
        }
    }
    return false;
}

bool ResultList::AtEnd(int i) {
    std::shared_ptr<Result> p_r = results_[i];
    return vector_current_pos_[i] == p_r->End();
}

void ResultList::Clear() {
    results_.clear();
    vector_current_pos_.clear();
}

int ResultList::Size() {
    return results_.size();
}
