#include <rdf-tdaa/rdf-tdaa.hpp>
#include "exec/args_parser.hpp"

void Build(const std::unordered_map<std::string, std::string>& arguments) {
    std::string db_name = arguments.at("name");
    std::string data_file = arguments.at("file");
    rdftdaa::RDFTDAA::Create(db_name, data_file);
}

void Query(const std::unordered_map<std::string, std::string>& arguments) {
    std::string db_name;
    std::string sparql_file;
    if (arguments.count("name"))
        db_name = arguments.at("name");
    if (arguments.count("file"))
        sparql_file = arguments.at("file");

    rdftdaa::RDFTDAA::Query(db_name, sparql_file);
}

void Server(const std::unordered_map<std::string, std::string>& arguments) {
    std::string ip = arguments.at("ip");
    std::string port = arguments.at("port");
    std::string db = arguments.at("name");
    rdftdaa::RDFTDAA::Server(ip, port, db);
}

struct EnumClassHash {
    template <typename T>
    std::size_t operator()(T t) const {
        return static_cast<std::size_t>(t);
    }
};

std::unordered_map<ArgsParser::CommandT,
                   void (*)(const std::unordered_map<std::string, std::string>&),
                   EnumClassHash>
    selector;

int main(int argc, char** argv) {
    selector = {{ArgsParser::CommandT::kBuild, &Build},
                {ArgsParser::CommandT::kQuery, &Query},
                {ArgsParser::CommandT::kServer, &Server}};

    auto parser = ArgsParser();
    auto command = parser.Parse(argc, argv);
    auto arguments = parser.Arguments();
    selector[command](arguments);
    return 0;
}
