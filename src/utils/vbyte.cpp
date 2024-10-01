#include "rdf-tdaa/utils/vbyte.hpp"
#include <climits>
#include <fstream>
#include <iostream>
#include "streamvbyte.h"

std::pair<uint8_t*, uint> Compress(uint* data, uint size) {
    if (size > UINT_MAX)
        std::cout << "error size: " << size << std::endl;

    uint max_compressed_length = streamvbyte_max_compressedbytes(size);
    uint8_t* compressed_buffer = new uint8_t[max_compressed_length];

    uint compressed_length = streamvbyte_encode(data, size, compressed_buffer);  // encoding

    return {compressed_buffer, compressed_length};
}

void CompressAndSave(uint* data, uint size, std::string filename) {
    std::ofstream outfile(filename, std::ios::binary);

    auto [compressed_buffer, compressed_length] = Compress(data, size);

    outfile.write(reinterpret_cast<const char*>(&size), sizeof(size));
    outfile.write(reinterpret_cast<const char*>(&compressed_length), sizeof(compressed_length));
    outfile.write(reinterpret_cast<const char*>(compressed_buffer), compressed_length);
    outfile.close();

    delete[] compressed_buffer;
}

uint32_t* Decompress(uint8_t* compressed_buffer, uint total_length) {
    uint32_t* recovdata = new uint32_t[total_length];
    streamvbyte_decode(compressed_buffer, recovdata, total_length);
    delete[] compressed_buffer;
    return recovdata;
}

std::pair<uint*, uint> LoadAndDecompress(std::string filename) {
    uint total_length;
    uint compressed_length;
    uint8_t* compressed_buffer;

    std::ifstream infile(filename, std::ios::binary);
    infile.read(reinterpret_cast<char*>(&total_length), sizeof(total_length));
    infile.read(reinterpret_cast<char*>(&compressed_length), sizeof(compressed_length));
    compressed_buffer = new uint8_t[compressed_length];
    infile.read(reinterpret_cast<char*>(compressed_buffer), compressed_length);
    infile.close();

    return {Decompress(compressed_buffer, total_length), total_length};
}