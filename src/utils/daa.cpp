#include "rdf-tdaa/utils/daa.hpp"

DAA::DAA(std::vector<std::vector<uint>>& arrays) {
    create(arrays);
}

DAA::~DAA() {
    delete[] levels_;
    delete[] level_end_;
    delete[] array_end_;
}

void DAA::create(std::vector<std::vector<uint>>& arrays) {
    uint level_cnt = 0;
    for (auto& array : arrays) {
        if (array.size() > level_cnt)
            level_cnt = array.size();
    }

    uint* level_size = (uint*)malloc(sizeof(uint) * level_cnt);
    for (uint i = 0; i < level_cnt; i++)
        level_size[i] = 0;

    data_cnt_ = 0;
    for (uint i = 0; i < arrays.size(); i++) {
        for (uint j = 0; j < arrays[i].size(); j++) {
            level_size[j]++;
            data_cnt_++;
        }
    }

    level_end_ = (char*)malloc((data_cnt_ + 7) / 8);
    levels_ = (uint*)malloc(sizeof(uint) * data_cnt_);
    array_end_ = (char*)malloc((data_cnt_ + 7) / 8);
    uint* contB = (uint*)malloc(sizeof(uint) * level_cnt + 1);

    for (uint i = 0; i < (data_cnt_ + 7) / 8; i++) {
        level_end_[i] = 0;
        array_end_[i] = 0;
    }

    // uint indexLevel = 0;
    contB[0] = 0;
    for (uint j = 0; j < level_cnt; j++) {
        contB[j + 1] = contB[j] + level_size[j];
        bit_set(level_end_, contB[j + 1] - 1);
    }

    uint top_level;
    for (uint i = 0; i < arrays.size(); i++) {
        top_level = arrays[i].size() - 1;
        for (uint j = 0; j <= top_level; j++) {
            levels_[contB[j]] = arrays[i][j];
            contB[j]++;
        }
        bit_set(array_end_, contB[top_level] - 1);
    }

    free(contB);
    free(level_size);

    // for (uint pos = 0; pos <= data_cnt_; pos++)
    //     std::cout << get(array_end_, pos) << " ";
    // std::cout << std::endl;
    // for (uint pos = 0; pos <= data_cnt_; pos++)
    //     std::cout << get(level_end_, pos) << " ";
    // std::cout << std::endl << std::endl;
    // uint last_end = 0;
    // One one = One(level_end_, 0, data_cnt_);
    // for (uint i = 0; i < 6; i++) {
    //     uint end = one.Next();
    //     for (uint pos = last_end; pos <= end; pos++)
    //         std::cout << get(array_end_, pos) << " ";
    //     std::cout << std::endl;
    //     last_end = end + 1;
    // }
    // std::cout << std::endl;
}