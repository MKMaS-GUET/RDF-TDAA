#ifndef ARGS_PARSER_HPP
#define ARGS_PARSER_HPP

#include <algorithm>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

class ArgsParser {
   public:
    enum CommandT {
        kNone,
        kBuild,
        kQuery,
        kServer,
    };

    const std::string arg_name_ = "name";
    const std::string arg_file_ = "file";
    const std::string arg_ip_ = "ip";
    const std::string arg_port_ = "port";
    const std::string arg_thread_num_ = "thread_num";
    const std::string arg_chunk_size_ = "chunk_size";

   private:
    std::unordered_map<std::string, CommandT> position_ = {
        {"-h", CommandT::kNone},     {"--help", CommandT::kNone},   {"build", CommandT::kBuild},
        {"query", CommandT::kQuery}, {"server", CommandT::kServer},
    };

    std::unordered_map<std::string, void (ArgsParser::*)(const std::unordered_map<std::string, std::string>&)>
        selector_ = {
            {"build", &ArgsParser::Build},
            {"query", &ArgsParser::Query},
            {"server", &ArgsParser::Server},
    };

    const std::string help_info_ =
        "Usage: rdftdaa [COMMAND] [OPTIONS]\n"
        "\n"
        "Commands:\n"
        "  build      Build an RDF database.\n"
        "  query      Query an RDF database.\n"
        "  server     Start an RDF database.\n"
        "\n"
        "Options:\n"
        "  -h, --help  Show this help message and exit.\n"
        "\n"
        "Commands:\n"
        "\n"
        "  build\n"
        "    Build an RDF database.\n"
        "\n"
        "    Usage: rdftdaa build [OPTIONS]\n"
        "\n"
        "    Options:\n"
        "      -d, --database <NAME>   Specify the name of the database.\n"
        "      -f, --file <FILE>       Specify the input file to build the database.\n"
        "\n"
        "  query\n"
        "    Query an RDF database.\n"
        "\n"
        "    Usage: rdftdaa query [OPTIONS]\n"
        "\n"
        "    Options:\n"
        "      -d, --database <NAME>   Specify the name of the database.\n"
        "      -f, --file <FILE>       Specify the file containing the query.\n"
        "\n"
        "  server\n"
        "    Start an RDF endpoint.\n"
        "\n"
        "    Usage: rdftdaa server [OPTIONS]\n"
        "\n"
        "    Options:\n"
        "      -d, --database <NAME>   Specify the name of the database.\n"
        "      --ip <IP ADDRESS>       Specify the IP address for the endpoint.\n"
        "      --port <PORT>           Specify the port for the endpoint.\n";

    std::unordered_map<std::string, std::string> arguments_;

   private:
    void Build(const std::unordered_map<std::string, std::string>& args);

    void Query(const std::unordered_map<std::string, std::string>& args);

    void Server(const std::unordered_map<std::string, std::string>& args);

    inline bool IsNumber(const std::string& s) {
        return std::all_of(s.begin(), s.end(), [](char c) { return std::isdigit(c); });
    }

   public:
    CommandT Parse(int argc, char** argv);

    const std::unordered_map<std::string, std::string>& Arguments() const;
};

#endif  // ARGS_PARSER_HPP
