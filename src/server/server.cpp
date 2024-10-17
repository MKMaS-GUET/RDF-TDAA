#include "rdf-tdaa/server/server.hpp"
#include "rapidjson/writer.h"
#include "rdf-tdaa/parser/sparql_parser.hpp"
#include "rdf-tdaa/query/plan_generator.hpp"
#include "rdf-tdaa/query/query_executor.hpp"

void Endpoint::query(const httplib::Request& req, httplib::Response& res) {
    std::string sparql = req.get_param_value("query");

    if (db_name != "" && db_index != 0) {
        auto exec_start = std::chrono::high_resolution_clock::now();

        auto parser = std::make_shared<SPARQLParser>(sparql);

        auto query_plan = std::make_shared<PlanGenerator>(db_index, parser);
        auto executor =
            std::make_shared<QueryExecutor>(db_index, query_plan, parser->Limit(), db_index->shared_cnt());
        if (!query_plan->zero_result())
            executor->Query();

        std::vector<std::vector<uint>>& results_id = executor->result();

        std::cout << results_id.size() << " ";

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

            if (query_plan->distinct_predicate()) {
                phmap::flat_hash_set<uint> distinct_predicate;
                for (auto it = results_id.begin(); it != last; ++it) {
                    const auto& item = *it;
                    for (const auto& idx : variable_indexes)
                        distinct_predicate.insert(item[idx.priority]);
                }
                for (auto it = distinct_predicate.begin(); it != distinct_predicate.end(); ++it) {
                    writer.StartArray();
                    writer.String(db_index->ID2String(*it, SPARQLParser::Term::Positon::kPredicate));
                    writer.EndArray();
                }
            } else {
                const auto& modifier = parser->project_modifier();
                if (modifier.modifier_type == SPARQLParser::ProjectModifier::Distinct) {
                    uint variable_cnt = query_plan->value2variable().size();

                    if (variable_cnt != variable_indexes.size()) {
                        std::vector<uint> not_projection_variable_index;
                        for (uint i = 0; i < variable_cnt; i++)
                            not_projection_variable_index.push_back(i);

                        std::set<uint> indexes_to_remove;
                        for (const auto& idx : variable_indexes)
                            indexes_to_remove.insert(idx.priority);

                        not_projection_variable_index.erase(
                            std::remove_if(not_projection_variable_index.begin(),
                                           not_projection_variable_index.end(),
                                           [&indexes_to_remove](uint value) {
                                               return indexes_to_remove.count(value) > 0;
                                           }),
                            not_projection_variable_index.end());

                        for (uint result_id = 0; result_id < results_id.size(); result_id++) {
                            for (const auto& idx : not_projection_variable_index)
                                results_id[result_id][idx] = 0;
                        }
                        std::sort(results_id.begin(), results_id.end());
                    }

                    last = std::unique(
                        results_id.begin(), results_id.end(),
                        [&](const std::vector<uint>& a, const std::vector<uint>& b) {
                            return std::all_of(
                                variable_indexes.begin(), variable_indexes.end(),
                                [&](PlanGenerator::Variable v) { return a[v.priority] == b[v.priority]; });
                        });
                }
                uint size = last - results_id.begin();

                for (uint rid = 0; rid < size; ++rid) {
                    const auto& item = results_id[rid];
                    writer.StartArray();
                    for (uint i = 0; i < variable_indexes.size(); i++) {
                        auto& idx = variable_indexes[i];
                        writer.String(db_index->ID2String(item[idx.priority], idx.position));
                    }
                    writer.EndArray();
                }
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

        auto exec_finish = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> diff = exec_finish - exec_start;
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

    std::string base_url = "/rdftdaa";

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