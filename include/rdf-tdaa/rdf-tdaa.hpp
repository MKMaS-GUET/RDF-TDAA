#ifndef RDF_TDAA_HPP
#define RDF_TDAA_HPP

#include <string>

namespace rdftdaa {

class RDFTDAA {
   public:
    RDFTDAA() = delete;

    ~RDFTDAA() = delete;

    static void Create(const std::string& db_name, const std::string& data_file);

    static void Query(const std::string& db_name, const std::string& data_file);

    static void Server(const std::string& ip, const std::string& port, const std::string& db);
};

}  // namespace rdftdaa

#endif
