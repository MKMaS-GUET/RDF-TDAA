#ifndef QUERY_EXECUTOR_HPP
#define QUERY_EXECUTOR_HPP

// #include <parallel_hashmap/phmap.h>
#include <chrono>
#include <string>
#include <vector>
#include "rdf-tdaa/index/index_retriever.hpp"
#include "rdf-tdaa/query/plan_generator.hpp"

class QueryExecutor {
    struct Stat {
        bool at_end_;
        int level_;
        // 用于记录每一个 level_ 的 candidate_result_ 已经处理过的结果的 id
        std::vector<uint> indices_;
        std::vector<uint> current_tuple_;
        std::vector<std::shared_ptr<std::vector<uint>>> candidate_result_;
        std::vector<std::vector<uint>> result_;
        std::vector<std::vector<PlanGenerator::Item>> plan_;

        Stat(const std::vector<std::vector<PlanGenerator::Item>>& p);

        Stat(const Stat& other);

        Stat& operator=(const Stat& other);
    };

    Stat stat_;
    std::shared_ptr<IndexRetriever> index_;
    std::vector<std::vector<size_t>>& other_type_;
    std::vector<std::vector<size_t>>& none_type_;
    std::vector<std::vector<std::shared_ptr<std::vector<uint>>>>& prestores_;
    uint limit_;
    uint shared_cnt_;
    std::chrono::duration<double, std::milli> query_duration_;

    // hash_map<std::string, std::shared_ptr<std::vector<uint>>> _pre_join_result;
    std::vector<std::shared_ptr<std::vector<uint>>> _pre_join_result;

    std::shared_ptr<std::vector<uint>> LeapfrogJoin(ResultList& indexes);

    bool PreJoin();

    void Down(Stat& stat);

    void Up(Stat& stat);

    void Next(Stat& stat);

    void EnumerateItems(Stat& stat);

    bool UpdateCurrentTuple(Stat& stat);

    bool SearchPredicatePath(Stat& stat, uint entity);

   public:
    QueryExecutor(std::shared_ptr<IndexRetriever> index,
                  std::shared_ptr<PlanGenerator>& plan,
                  uint limit,
                  uint shared_cnt);

    void Query();

    double query_duration();

    std::vector<std::vector<uint>>& result();
};

#endif  // QUERY_EXECUTOR_HPP
