#ifndef PLAN_GENERATOR_HPP
#define PLAN_GENERATOR_HPP

// #include <parallel_hashmap/phmap.h>
#include <climits>
#include <numeric>  // 包含 accumulate 函数
#include <string>
#include <unordered_map>
#include <vector>

#include "rdf-tdaa/index/index_retriever.hpp"
#include "rdf-tdaa/utils/result_list.hpp"

using Result = ResultList::Result;
using AdjacencyList = std::unordered_map<std::string, std::vector<std::pair<std::string, uint>>>;

class PlanGenerator {
   public:
    struct Item {
        enum TypeT { kPS, kPO, kNone };
        TypeT search_type_;            // mark the current search type
        uint search_code_;             // search this code from corresponding index according to
                                       // `curr_search_type_`
        size_t candidate_result_idx_;  // the next value location index
        // 一对迭代器，第一个是起始位置，第二个是结束位置
        std::shared_ptr<Result> search_result_;

        Item() = default;

        Item(const Item& other);

        Item& operator=(const Item& other);
    };

   private:
    std::vector<std::vector<Item>> query_plan_;
    // v -> (priority, pos)
    hash_map<std::string, std::pair<uint, Pos>> variable_metadata_;
    std::vector<std::vector<size_t>> other_type_;
    std::vector<std::vector<size_t>> none_type_;
    std::vector<std::vector<std::shared_ptr<Result>>> prestores_;
    bool zero_result_ = false;

   public:
    PlanGenerator(const std::shared_ptr<IndexRetriever>& index,
                  const std::vector<std::vector<std::string>>& triple_list);

    void DFS(const AdjacencyList& graph,
             std::string vertex,
             hash_map<std::string, bool>& visited,
             AdjacencyList& tree,
             std::vector<std::string>& currentPath,
             std::vector<std::vector<std::string>>& allPaths);

    std::vector<std::vector<std::string>> FindAllPathsInGraph(const AdjacencyList& graph,
                                                              const std::string& root);

    void Generate(const std::shared_ptr<IndexRetriever>& index,
                  const std::vector<std::vector<std::string>>& triple_list);

    void GenPlanTable(const std::shared_ptr<IndexRetriever>& index,
                      const std::vector<std::vector<std::string>>& triple_list,
                      std::vector<std::string>& variables);

    std::vector<std::pair<uint, Pos>> MappingVariable(const std::vector<std::string>& variables);

    std::vector<std::vector<Item>>& query_plan();
    hash_map<std::string, std::pair<uint, Pos>>& variable_metadata();
    std::vector<std::vector<size_t>>& other_type();
    std::vector<std::vector<size_t>>& none_type();
    std::vector<std::vector<std::shared_ptr<Result>>>& prestores();
    bool zero_result();
};

#endif  // COMBINED_CODE_INDEX_GEN_PLAN_HPP
