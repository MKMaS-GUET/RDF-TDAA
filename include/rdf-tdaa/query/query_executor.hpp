#ifndef QUERY_EXECUTOR_HPP
#define QUERY_EXECUTOR_HPP

#include <chrono>
#include <string>
#include <vector>
#include "rdf-tdaa/index/index_retriever.hpp"
#include "rdf-tdaa/query/plan_generator.hpp"

class QueryExecutor {
    using PType = PlanGenerator::Item::PType;
    using RType = PlanGenerator::Item::RType;

    struct Stat {
        bool at_end;
        int level;
        // 用于记录每一个 level_ 的 candidate_result_ 已经处理过的结果的 id
        std::vector<uint> current_tuple;
        std::vector<std::span<uint>> candidate_value;
        std::vector<uint> candidate_indices;
        std::shared_ptr<std::vector<std::vector<uint>>> result;
        std::vector<std::vector<PlanGenerator::Item>> plan;

        Stat(const std::vector<std::vector<PlanGenerator::Item>>& p);

        Stat(const Stat& other);

        Stat& operator=(const Stat& other);
    };

    Stat stat_;
    std::shared_ptr<IndexRetriever> index_;
    std::vector<std::vector<uint>>& filled_item_indices_;
    std::vector<std::vector<uint>>& empty_item_indices_;
    std::vector<std::vector<std::span<uint>>>& pre_results_;
    std::vector<std::span<uint>> pre_join_;
    uint limit_;
    uint shared_cnt_;
    bool skip_pre_result_;

    std::chrono::duration<double, std::milli> query_duration_;

    std::span<uint> static LeapfrogJoin(JoinList& lists);

    bool PreJoin();

    void Down(Stat& stat);

    void Up(Stat& stat);

    void Next(Stat& stat);

    void GenCandidateValue(Stat& stat);

    bool UpdateCurrentTuple(Stat& stat);

    bool FillEmptyItem(Stat& stat, uint entity);

   public:
    std::span<uint> static LeapfrogJoin(const std::vector<std::span<uint>>& lists);

    QueryExecutor(std::shared_ptr<IndexRetriever> index,
                  std::shared_ptr<PlanGenerator>& plan,
                  uint limit,
                  uint shared_cnt);

    void Query();

    double query_duration();

    std::vector<std::vector<uint>>& result();
};

#endif  // QUERY_EXECUTOR_HPP
