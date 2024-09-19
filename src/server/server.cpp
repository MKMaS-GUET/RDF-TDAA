#include "rdf-tdaa/server/server.hpp"

void Endpoint::query(const httplib::Request& req, httplib::Response& res) {
    std::string sparql = req.get_param_value("query");

    if (db_name != "" && db_index != 0) {
        auto exec_start = std::chrono::high_resolution_clock::now();

        auto parser = std::make_shared<SPARQLParser>(sparql);
        auto query_plan = std::make_shared<PlanGenerator>(db_index, parser->TriplePatterns());
        auto executor =
            std::make_shared<QueryExecutor>(db_index, query_plan, parser->Limit(), db_index->shared_cnt());
        if (!query_plan->zero_result())
            executor->Query();
        std::vector<std::vector<uint>>& results_id = executor->result();

        std::chrono::duration<double, std::milli> diff;
        auto exec_finish = std::chrono::high_resolution_clock::now();
        diff = exec_finish - exec_start;
        std::cout << results_id.size() << " ";
        std::cout << diff.count() << " ";

        auto result_start = std::chrono::high_resolution_clock::now();

        rapidjson::StringBuffer result;
        rapidjson::Writer<rapidjson::StringBuffer> writer(result);

        std::vector<std::string> variables = parser->ProjectVariables();

        writer.StartObject();

        writer.Key("head");
        writer.StartObject();
        writer.Key("vars");
        writer.StartArray();
        for (uint i = 0; i < variables.size(); i++)
            writer.String(variables[i].c_str());
        writer.EndArray();
        writer.EndObject();

        writer.Key("results");
        writer.StartObject();
        writer.Key("bindings");
        writer.StartArray();

        if (results_id.size()) {
            const auto variable_indexes = query_plan->MappingVariable(variables);

            auto last = results_id.end();
            const auto& modifier = parser->project_modifier();
            if (modifier.modifier_type_ == SPARQLParser::ProjectModifier::Distinct) {
                last = std::unique(results_id.begin(), results_id.end(),
                                   [&](const std::vector<uint>& a, const std::vector<uint>& b) {
                                       return std::all_of(variable_indexes.begin(), variable_indexes.end(),
                                                          [&](PlanGenerator::Variable v) {
                                                              return a[v.priority_] == b[v.priority_];
                                                          });
                                   });
            }
            uint size = last - results_id.begin();

            for (uint rid = 0; rid < size; ++rid) {
                const auto& item = results_id[rid];
                writer.StartArray();
                for (uint i = 0; i < variable_indexes.size(); i++) {
                    auto& idx = variable_indexes[i];
                    writer.String(db_index->ID2String(item[idx.priority_], idx.position_));
                }
                writer.EndArray();
            }
        }

        writer.EndArray();
        writer.EndObject();
        writer.EndObject();

        // std::string result;
        // result += "{\"head\": {\"vars\": [";
        // for (uint i = 0; i < variables.size(); i++) {
        //     result += '\"' + variables[i];
        //     if (i != variables.size() - 1)
        //         result += "\",";
        //     else
        //         result += '\"';
        // }
        // result += "]}, \"results\": {\"bindings\": [";

        // if (results_id.size()) {
        //     auto last = results_id.end();
        //     const auto& modifier = parser->project_modifier();
        //     if (modifier.modifier_type_ == SPARQLParser::ProjectModifier::Distinct) {
        //         last = std::unique(results_id.begin(), results_id.end(),
        //                            [&](const std::vector<uint>& a, const std::vector<uint>& b) {
        //                                return std::all_of(
        //                                    variable_indexes.begin(), variable_indexes.end(),
        //                                    [&](std::pair<uint, Pos> i) { return a[i.first] ==
        //                                    b[i.first];
        //                                    });
        //                            });
        //     }
        //     uint size = last - results_id.begin();

        //     auto& idx = variable_indexes[0];
        //     uint est_size =
        //         results_id.size() * db_index->ID2String(results_id[0][idx.first], idx.second).size();

        //     result.reserve(est_size);
        //     for (uint rid = 0; rid < size; ++rid) {
        //         const auto& item = results_id[rid];
        //         result += '[';
        //         for (uint i = 0; i < variable_indexes.size(); i++) {
        //             auto& idx = variable_indexes[i];
        //             result += db_index->ID2String(item[idx.first], idx.second);
        //             if (i != variable_indexes.size() - 1)
        //                 result += ',';
        //         }
        //         if (rid != size - 1)
        //             result += "],";
        //         else
        //             result += ']';
        //     }
        // }
        // result += "]}}";
        // res.set_content(result, "application/sparql-results+json;charset=utf-8");

        res.set_content(result.GetString(), "application/sparql-results+json;charset=utf-8");
        auto result_finish = std::chrono::high_resolution_clock::now();
        diff = result_finish - result_start;
        std::cout << diff.count() << std::endl;
    }
}

bool Endpoint::start_server(const std::string& ip, const std::string& port, const std::string& db) {
    std::cout << "Running at:" + ip + ":" << port << std::endl;

    httplib::Server svr;

    svr.set_default_headers({{"Access-Control-Allow-Origin", "*"},
                             {"Access-Control-Allow-Methods", "POST, GET, PUT, OPTIONS, DELETE"},
                             {"Access-Control-Max-Age", "3600"},
                             {"Access-Control-Allow-Headers", "*"},
                             {"Content-Type", "application/json;charset=utf-8"}});

    svr.set_base_dir("./");

    std::string base_url = "/epei";

    db_index = std::make_shared<IndexRetriever>(db);
    db_name = db;

    svr.Get(base_url + "/sparql", [this](const httplib::Request& req, httplib::Response& res) {
        this->query(req, res);
    });  // query on RDF
    svr.Post(base_url + "/sparql", [this](const httplib::Request& req, httplib::Response& res) {
        this->query(req, res);
    });  // query on RDF
    svr.Options(base_url + "/sparql",
                [](const httplib::Request& req, httplib::Response& res) { res.status = 200; });

    // disconnect
    svr.Get(base_url + "/disconnect", [&](const httplib::Request& req, httplib::Response& res) {
        std::cout << "disconnection from http://" << req.remote_addr << ":" << req.remote_port << std::endl;
        rapidjson::StringBuffer result;
        rapidjson::Writer<rapidjson::StringBuffer> writer(result);

        writer.StartObject();
        writer.Key("code");
        writer.Uint(1);
        writer.Key("message");
        writer.String("Disconnected");
        writer.EndObject();

        svr.stop();
        res.set_content(result.GetString(), "text/plain;charset=utf-8");
    });
    svr.listen(ip, std::stoi(port));
    return 0;
}