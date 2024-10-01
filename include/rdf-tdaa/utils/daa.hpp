#ifndef DAA_HPP
#define DAA_HPP

#include <iostream>
#include <memory>
#include <vector>

#define bit_set(bits, offset) ((bits)[(offset) / 8] |= (1 << (7 - (offset) % 8)))
#define bit_get(bits, offset) (((bits)[(offset) / 8] >> (7 - (offset) % 8)) & 1)

struct DAA {
    uint data_cnt;
    uint* levels;
    char* level_end;
    char* array_end;

    void create(std::vector<std::vector<uint>>& arrays);

   public:
    DAA(std::vector<std::vector<uint>>& arrays);
    ~DAA();
};

#endif