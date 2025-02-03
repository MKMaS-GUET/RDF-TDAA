#include <cstdint>
#include <string>

std::pair<uint8_t*, uint> Compress(uint* data, uint size);

void CompressAndSave(uint* data, uint size, std::string filename);

uint32_t* Decompress(uint8_t* compressed_buffer, uint total_length);

std::pair<uint*, uint> LoadAndDecompress(std::string filename);