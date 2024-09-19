#ifndef RESULT_LIST_HPP
#define RESULT_LIST_HPP

#include <iterator>
#include <memory>
#include <string>
#include <vector>

class ResultList {
   public:
   private:
    std::vector<std::shared_ptr<std::vector<uint>>> results_;

    std::vector<std::vector<uint>::iterator> vector_current_pos_;

   public:
    ResultList();

    void AddVector(std::shared_ptr<std::vector<uint>> range);

    bool AddVectors(std::vector<std::shared_ptr<std::vector<uint>>> ranges);

    std::shared_ptr<std::vector<uint>> Shortest();

    void UpdateCurrentPostion();

    void Seek(int i, uint val);

    // 对range顺序排序后，获取第i个range第一个值
    uint GetCurrentValOfRange(int i);

    // 更新range的起始迭代器
    void NextVal(int i);

    std::shared_ptr<std::vector<uint>> GetRangeByIndex(int i);

    bool HasEmpty();

    bool AtEnd(int i);

    void Clear();

    int Size();
};

#endif