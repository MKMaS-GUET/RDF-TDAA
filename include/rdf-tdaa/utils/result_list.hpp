#ifndef RESULT_LIST_HPP
#define RESULT_LIST_HPP

#include <iterator>
#include <memory>
#include <string>
#include <vector>

class ResultList {
   public:
    class Result {
        uint* start_;
        uint size_;

       public:
        int id = -1;
        Result();

        Result(uint* start, uint size);

        ~Result();

        class Iterator {
            uint* ptr_;

           public:
            Iterator();
            Iterator(uint* p);
            Iterator(const Iterator& it);

            Iterator& operator++();
            Iterator operator++(int);
            uint operator-(Result::Iterator r_it);
            Iterator operator-(int num);
            Iterator operator+(int num);
            bool operator==(const Iterator& rhs) const;
            bool operator!=(const Iterator& rhs) const;
            bool operator<(const Iterator& rhs) const;
            uint& operator*();
        };

        Iterator Begin();
        Iterator End();
        uint& operator[](uint i);
        uint Size();
    };

   private:
    std::vector<std::shared_ptr<Result>> results_;

    std::vector<Result::Iterator> vector_current_pos_;

   public:
    ResultList();

    void AddVector(std::shared_ptr<Result> range);

    bool AddVectors(std::vector<std::shared_ptr<Result>> ranges);

    std::shared_ptr<Result> Shortest();

    void UpdateCurrentPostion();

    void Seek(int i, uint val);

    // 对range顺序排序后，获取第i个range第一个值
    uint GetCurrentValOfRange(int i);

    // 更新range的起始迭代器
    void NextVal(int i);

    std::shared_ptr<Result> GetRangeByIndex(int i);

    bool HasEmpty();

    bool AtEnd(int i);

    void Clear();

    int Size();
};

#endif