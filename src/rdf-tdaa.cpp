#include <rdf-tdaa/rdf-tdaa.hpp>
#include "rdf-tdaa/index/index_builder.hpp"
#include "rdf-tdaa/index/index_retriever.hpp"
#include "rdf-tdaa/parser/sparql_parser.hpp"
#include "rdf-tdaa/query/plan_generator.hpp"
#include "rdf-tdaa/query/query_executor.hpp"
#include "rdf-tdaa/server/server.hpp"

uint QueryResult(std::vector<std::vector<uint>>& result,
                 const std::shared_ptr<IndexRetriever> index,
                 const std::shared_ptr<PlanGenerator> query_plan,
                 const std::shared_ptr<SPARQLParser> parser) {
    const auto& modifier = parser->project_modifier();
    // project_variables 是要输出的变量顺序
    // 而 result 的变量顺序是计划生成中的变量排序
    // 所以要获取每一个要输出的变量在 result 中的位置
    for (uint i = 0; i < parser->ProjectVariables().size(); i++)
        std::cout << parser->ProjectVariables()[i] << " ";
    std::cout << std::endl;

    if (query_plan->zero_result())
        return 0;

    const auto variable_indexes = query_plan->MappingVariable(parser->ProjectVariables());

    if (query_plan->distinct_predicate()) {
        phmap::flat_hash_set<uint> distinct_predicate;
        for (auto it = result.begin(); it != result.end(); ++it) {
            const auto& item = *it;
            for (const auto& idx : variable_indexes)
                distinct_predicate.insert(item[idx.priority]);
        }
        for (auto it = distinct_predicate.begin(); it != distinct_predicate.end(); ++it)
            std::cout << index->ID2String(*it, SPARQLParser::Term::Positon::kPredicate) << std::endl;
        return distinct_predicate.size();
    } else {
        auto last = result.end();

        uint cnt = 0;
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
                    std::remove_if(
                        not_projection_variable_index.begin(), not_projection_variable_index.end(),
                        [&indexes_to_remove](uint value) { return indexes_to_remove.count(value) > 0; }),
                    not_projection_variable_index.end());

                for (uint result_id = 0; result_id < result.size(); result_id++) {
                    for (const auto& idx : not_projection_variable_index)
                        result[result_id][idx] = 0;
                }
                std::sort(result.begin(), result.end());
            }

            last = std::unique(result.begin(), result.end(),
                               // 判断两个列表 a 和 b 是否相同，
                               [&](const std::vector<uint>& a, const std::vector<uint>& b) {
                                   // std::all_of 可以用来判断数组中的值是否都满足一个条件
                                   return std::all_of(variable_indexes.begin(), variable_indexes.end(),
                                                      // 判断依据是，列表中的每一个元素都相同
                                                      [&](PlanGenerator::Variable v) {
                                                          return a[v.priority] == b[v.priority];
                                                      });
                               });
        }
        for (auto it = result.begin(); it != last; ++it) {
            const auto& item = *it;
            for (const auto& idx : variable_indexes) {
                std::cout << index->ID2String(item[idx.priority], idx.position) << " ";
            }
            std::cout << std::endl;
            cnt++;
        }
        return cnt;
    }
    return 0;
}

namespace rdftdaa {

void RDFTDAA::Create(const std::string& db_name, const std::string& data_file) {
    auto beg = std::chrono::high_resolution_clock::now();

    IndexBuilder builder(db_name, data_file);
    if (!builder.Build()) {
        std::cerr << "Building index data failed, terminal the process." << std::endl;
        exit(1);
    }

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> diff = end - beg;
    std::cout << "create " << db_name << " takes " << diff.count() << " ms." << std::endl;
}

void RDFTDAA::Query(const std::string& db_path, const std::string& data_file) {
    if (db_path != "" and data_file != "") {
        std::shared_ptr<IndexRetriever> index = std::make_shared<IndexRetriever>(db_path);
        std::ifstream in(data_file, std::ifstream::in);
        std::vector<std::string> sparqls;
        if (in.is_open()) {
            std::string line;
            std::string sparql;
            while (std::getline(in, sparql)) {
                sparqls.push_back(sparql);
            }
            in.close();
        }

        std::ios::sync_with_stdio(false);
        for (long unsigned int i = 0; i < sparqls.size(); i++) {
            std::string sparql = sparqls[i];

            if (sparqls.size() > 1) {
                std::cout << i + 1 << " ------------------------------------------------------------------"
                          << std::endl;
                std::cout << sparql << std::endl;
            }

            auto start = std::chrono::high_resolution_clock::now();
            auto parser = std::make_shared<SPARQLParser>(sparql);

            auto query_plan = std::make_shared<PlanGenerator>(index, parser);
            auto plan_end = std::chrono::high_resolution_clock::now();

            auto executor =
                std::make_shared<QueryExecutor>(index, query_plan, parser->Limit(), index->shared_cnt());
            if (!query_plan->zero_result())
                executor->Query();

            auto projection_start = std::chrono::high_resolution_clock::now();
            uint cnt = QueryResult(executor->result(), index, query_plan, parser);
            auto projection_finish = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double, std::milli> mapping_diff = projection_finish - projection_start;

            auto finish = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double, std::milli> diff = finish - start;

            std::chrono::duration<double, std::milli> plan_time = plan_end - start;

            std::cout << cnt << " result(s).\n";
            std::cout << "generate plan takes " << plan_time.count() << " ms.\n";
            std::cout << "execute takes " << executor->query_duration() << " ms.\n";
            std::cout << "projection takes " << mapping_diff.count() << " ms.\n";
            std::cout << "query cost " << diff.count() << " ms." << std::endl;
        }
        exit(0);
    }
}

void RDFTDAA::Server(const std::string& ip, const std::string& port, const std::string& db) {
    Endpoint e;

    e.start_server(ip, port, db);
}

}  // namespace rdftdaa