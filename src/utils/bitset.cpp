#include "rdf-tdaa/utils/bitset.hpp"

Bitset::Bitset() {};

Bitset::Bitset(ulong initial_size) : bit_size_(initial_size) {
    bits_.resize((initial_size + 63) / 64, 0);
}

Bitset::~Bitset() {
    std::vector<unsigned long>().swap(bits_);
}

bool Bitset::Get(ulong pos) {
    if (pos >= bit_size_)
        Resize(pos + 1);
    return (bits_[pos / 64] & (1UL << (pos % 64))) != 0;
}

void Bitset::Set(ulong pos) {
    if (pos >= bit_size_)
        Resize(pos + 1);
    bits_[pos / 64] |= (1UL << (pos % 64));
}

void Bitset::Unset(ulong pos) {
    if (pos >= bit_size_)
        Resize(pos + 1);
    bits_[pos / 64] &= ~(1UL << (pos % 64));
}

void Bitset::Resize(ulong new_size) {
    bit_size_ = new_size;
    bits_.resize((new_size + 63) / 64, 0);
}

ulong Bitset::Size() const {
    return bit_size_;
}