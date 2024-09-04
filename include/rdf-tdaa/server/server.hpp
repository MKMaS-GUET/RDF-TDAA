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

#include "rapidjson/writer.h"
#include "rdf-tdaa/index/index_builder.hpp"
#include "rdf-tdaa/parser/sparql_parser.hpp"
#include "rdf-tdaa/query/plan_generator.hpp"
#include "rdf-tdaa/query/query_executor.hpp"

class Endpoint {
   public:
    std::string db_name;
    std::shared_ptr<IndexRetriever> db_index;

    Endpoint() {}

    void query(const httplib::Request& req, httplib::Response& res);

    bool start_server(const std::string& ip, const std::string& port, const std::string& db);
};

#endif