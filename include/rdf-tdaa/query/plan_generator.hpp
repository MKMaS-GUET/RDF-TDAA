#ifndef PLAN_GENERATOR_HPP
#define PLAN_GENERATOR_HPP

#include <climits>
#include <deque>
#include <string>
#include <unordered_map>
#include <vector>

#include "rdf-tdaa/index/index_retriever.hpp"
#include "rdf-tdaa/utils/join_list.hpp"

using AdjacencyList = std::unordered_map<std::string, std::vector<std::pair<std::string, uint>>>;

class PlanGenerator {
   public:
    struct Item {
        enum RType {
            kGetBySP,
            kGetByOP,
            kGetBySO,
            kOtherSet,
            kGetSPreSet,
            kGetOPreSet,
            kGetSSet,
            kGetOSet,
            kNone
        };
        enum PType { kPreSub, kPreObj, kSubject, kPredicate, kObject, kEmpty };

        RType retrieval_type;
        PType prestore_type;
        std::span<uint> index_result;

        uint triple_pattern_id;
        uint search_id;
        uint father_item_id;
        uint empty_item_level;

        Item() = default;

        Item(const Item& other);

        Item& operator=(const Item& other);
    };

    struct Variable {
        std::string value;
        ulong priority;
        uint count;
        SPARQLParser::Term::Positon position;

        Variable();
        Variable(std::string value);
        Variable(std::string value, ulong priority, SPARQLParser::Term::Positon position);

        Variable& operator=(const Variable& other);
    };

   private:
    bool debug_ = false;
    std::shared_ptr<IndexRetriever>& index_;
    std::shared_ptr<SPARQLParser>& sparql_parser_;
    std::vector<Variable> variable_order_;
    hash_map<std::string, Variable*> value2variable_;
    std::vector<std::vector<Item>> query_plan_;
    std::vector<std::vector<uint>> filled_item_indices_;
    std::vector<std::vector<uint>> empty_item_indices_;
    std::vector<std::vector<std::span<uint>>> pre_results_;
    bool distinct_predicate_ = false;
    bool zero_result_ = false;

    void SortVariables(AdjacencyList& query_graph_ud,
                       hash_map<std::string, Variable>& univariates,
                       hash_map<std::string, uint>& est_size);

    void GenPlanTable();

   public:
    PlanGenerator(std::shared_ptr<IndexRetriever>& index, std::shared_ptr<SPARQLParser>& sparql_parser);

    void DFS(const AdjacencyList& graph,
             std::string vertex,
             hash_map<std::string, bool>& visited,
             AdjacencyList& tree,
             std::deque<std::string>& current_path,
             std::vector<std::deque<std::string>>& all_paths);

    std::vector<std::deque<std::string>> FindAllPathsInGraph(const AdjacencyList& graph,
                                                             const std::string& root);

    void Generate();

    std::vector<Variable> MappingVariable(const std::vector<std::string>& variables);

    std::vector<std::vector<Item>>& query_plan();
    hash_map<std::string, Variable*>& value2variable();
    std::vector<std::vector<uint>>& filled_item_indices();
    std::vector<std::vector<uint>>& empty_item_indices();
    std::vector<std::vector<std::span<uint>>>& pre_results();
    bool zero_result();
    bool distinct_predicate();
};

#endif
