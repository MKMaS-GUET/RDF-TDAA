#ifndef JOIN_LIST_HPP
#define JOIN_LIST_HPP

#include <iterator>
#include <memory>
#include <span>
#include <string>
#include <vector>

class JoinList {
   public:
   private:
    std::vector<std::span<uint>> lists_;

    std::vector<std::span<uint>::iterator> list_current_pos_;

   public:
    JoinList();

    JoinList(std::vector<std::span<uint>>& lists);

    void AddList(const std::span<uint>& list);

    void AddLists(const std::vector<std::span<uint>>& lists);

    std::span<uint> Shortest();

    void UpdateCurrentPostion();

    void Seek(int i, uint val);

    // 对range顺序排序后，获取第i个range第一个值
    uint GetCurrentValOfList(int i);

    // 更新range的起始迭代器
    void NextVal(int i);

    std::span<uint> GetShortest();

    std::span<uint> GetListByIndex(int i);

    bool HasEmpty();

    bool AtEnd(int i);

    void Clear();

    int Size();
};

#endif