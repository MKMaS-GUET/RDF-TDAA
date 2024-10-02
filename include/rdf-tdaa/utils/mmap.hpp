#ifndef MMAP_HPP
#define MMAP_HPP

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <fstream>
#include <string>

template <typename T>
class MMap {
   private:
    void Create(std::string path, ulong size) {
        fd_ = open((path).c_str(), O_RDWR | O_CREAT, (mode_t)0600);

        if (fd_ == -1) {
            perror("Error opening file for mmap");
            exit(1);
        }

        if (size == 0 || ftruncate(fd_, size) == -1) {
            perror("Error truncating file for mmap");
            close(fd_);
            exit(1);
        }

        map_ = static_cast<T*>(mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0));
        if (map_ == MAP_FAILED) {
            perror("Error mapping file for mmap");
            close(fd_);
            exit(1);
        }
    }

   public:
    T* map_;
    int fd_;
    std::string path_;
    ulong size_;  // bytes
    ulong offset_;

    MMap() {}

    MMap(std::string path) : path_(path), offset_(0) {
        std::ifstream file(path, std::ios::binary | std::ios::ate);
        if (file.fail()) {
            perror("File not exist");
            exit(1);
        }
        size_ = static_cast<ulong>(file.tellg());
        file.close();

        Create(path, size_);
    }

    MMap(std::string path, ulong size) : path_(path), size_(size), offset_(0) { Create(path, size); }

    void Write(T data) {
        if (offset_ >= 0 && offset_ < size_ / sizeof(T)) {
            map_[offset_] = data;
            offset_++;
        }
    }

    T& operator[](ulong offset) {
        if (offset >= 0 && offset < size_ / sizeof(T)) {
            return map_[offset];
        }
        static T error = 0;

        return error;
    }

    void CloseMap() {
        if (size_) {
            if (msync(map_, size_, MS_SYNC) == -1) {
                perror("Error syncing memory to disk");
            }

            if (munmap(map_, size_) == -1) {
                perror("Error unmapping memory");
            }

            if (close(fd_) == -1) {
                perror("Error closing file descriptor");
            }
        }
    }
};

#endif