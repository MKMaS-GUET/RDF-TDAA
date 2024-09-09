#include <string>
#include "sys/types.h"

void CompressAndSave(uint* data, uint size, std::string filename);

std::pair<uint*, uint> LoadAndDecompress(std::string filename);