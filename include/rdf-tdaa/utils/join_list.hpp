#ifndef JOIN_LIST_HPP
#define JOIN_LIST_HPP

#include <iterator>
#include <memory>
#include <string>
#include <vector>

class JoinList {
   public:
   private:
    std::vector<std::shared_ptr<std::vector<uint>>> lists_;

    std::vector<std::vector<uint>::iterator> vector_current_pos_;

   public:
    JoinList();

    JoinList(std::vector<std::shared_ptr<std::vector<uint>>>& lists);

    void AddVector(std::shared_ptr<std::vector<uint>>& list);

    void AddVectors(std::vector<std::shared_ptr<std::vector<uint>>>& lists);

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