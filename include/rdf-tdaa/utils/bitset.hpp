#ifndef BIT_SET
#define BIT_SET

#include <vector>
#include "sys/types.h"

class Bitset {
   private:
    std::vector<unsigned long> bits_;
    ulong bit_size_;  // 总位数

   public:
    Bitset();
    Bitset(ulong initial_size);
    ~Bitset();

    bool Get(ulong pos);

    void Set(ulong pos);

    void Unset(ulong pos);

    void Resize(ulong new_size);

    ulong Size() const;
};

#endif