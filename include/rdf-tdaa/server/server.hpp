#ifndef SERVER_HPP
#define SERVER_HPP

#include <httplib.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <set>
#include <string>
#include <unordered_set>
#include <utility>

#include "rdf-tdaa/index/index_retriever.hpp"

class Endpoint {
   public:
    std::string db_name;
    std::shared_ptr<IndexRetriever> db_index;

    Endpoint() {}

    void query(const httplib::Request& req, httplib::Response& res);

    bool start_server(const std::string& ip, const std::string& port, const std::string& db);
};

#endif