#ifndef PLAN_GENERATOR_HPP
#define PLAN_GENERATOR_HPP

#include <deque>
#include <string>
#include <unordered_map>
#include <vector>

#include "rdf-tdaa/index/index_retriever.hpp"
#include "rdf-tdaa/utils/join_list.hpp"

using AdjacencyList = std::unordered_map<std::string, std::vector<std::pair<std::string, uint>>>;
using TripplePattern = std::vector<std::pair<std::array<SPARQLParser::Term, 3>, uint>>;

class PlanGenerator {
   public:
    /**
     * @struct Item
     * @brief Represents an item in the query plan generation process.
     *
     * The `Item` structure is used to define the properties and behavior of an
     * individual query plan item. It includes information about retrieval type,
     * prestore type, associated indices, and hierarchical relationships.
     */
    struct Item {
        /**
         * @enum RType
         * @brief Specifies the type of retrieval operation for the item, same as the retrieval operations in
         * index_retriever.cpp.
         */
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

        /**
         * @enum PType
         * @brief Specifies the type of prestore operation for the item.
         *
         * - `kPreSub`: Predicates' subject.
         * - `kPreObj`: Predicates' object.
         * - `kSubject`: Subject type.
         * - `kPredicate`: Predicate type.
         * - `kObject`: Object type.
         * - `kEmpty`: Empty type.
         */
        enum PType { kPreSub, kPreObj, kSubject, kPredicate, kObject, kEmpty };

        // The retrieval type of the item.
        RType retrieval_type;

        // The prestore type of the item.
        PType prestore_type;

        // Storing retrieval results associated with the item.
        std::span<uint> index_result;

        // The ID of the triple pattern associated with the item.
        uint triple_pattern_id;

        // The search ID associated with the item.
        uint search_id;

        // The ID of the parent item in the same triple pattern.
        uint father_item_id;

        // The level of the related item in the same triple pattern.
        uint empty_item_level;

        /**
         * @brief Default constructor for the `Item` structure.
         */
        Item();

        /**
         * @brief Copy constructor for the `Item` structure.
         * @param other The `Item` instance to copy from.
         */
        Item(const Item& other);

        /**
         * @brief Copy assignment operator for the `Item` structure.
         * @param other The `Item` instance to assign from.
         * @return A reference to the assigned `Item` instance.
         */
        Item& operator=(const Item& other);
    };

    /**
     * @struct Variable
     * @brief Represents a variable used in SPARQL query planning.
     */
    struct Variable {
        // The string representation of the variable's value.
        std::string value;

        // The priority of the variable.
        ulong priority;

        // The count of occurrences of the variable.
        uint count;

        // The position of the variable in the SPARQL triple pattern.
        SPARQLParser::Term::Positon position;

        Variable();
        Variable(std::string value);
        Variable(std::string value, ulong priority, SPARQLParser::Term::Positon position);

        Variable& operator=(const Variable& other);
    };

   private:
    bool debug_ = false;

    // A shared pointer to the IndexRetriever instance used for query plan generation.
    std::shared_ptr<IndexRetriever>& index_;

    // A vector storing the order of variables used in the query plan.
    std::vector<Variable> variable_order_;

    // A hash map mapping variable values (as strings) to their corresponding Variable objects.
    hash_map<std::string, Variable*> value2variable_;

    // A 2D vector representing the generated query plan, where each inner vector contains items
    // for aspecific step.
    std::vector<std::vector<Item>> query_plan_;

    // A 2D vector storing indices of filled items in the query plan.
    std::vector<std::vector<uint>> filled_item_indices_;

    // A 2D vector storing indices of empty items in the query plan.
    std::vector<std::vector<uint>> empty_item_indices_;

    // A 2D vector storing pre-retrieved results variable items in query plan.
    std::vector<std::vector<std::span<uint>>> pre_results_;

    // A flag indicating whether the a predicate has a distinct modifier in the query.
    bool distinct_predicate_ = false;

    // A flag indicating whether the query has zero results.
    bool zero_result_ = false;

    /**
     * @brief Performs a Depth-First Search (DFS) on the given graph to find all paths.
     *
     * @param graph The adjacency list representation of the graph.
     * @param vertex The current vertex being visited.
     * @param visited A hash map to track visited vertices.
     * @param tree The adjacency list representation of the DFS tree.
     * @param current_path A deque to store the current path being explored.
     * @param all_paths A vector to store all discovered paths.
     */
    void DFS(const AdjacencyList& graph,
             std::string vertex,
             hash_map<std::string, bool>& visited,
             AdjacencyList& tree,
             std::deque<std::string>& current_path,
             std::vector<std::deque<std::string>>& all_paths);

    /**
     * @brief Finds all paths (The vertices in all the paths should exactly cover all the vertices in the
     * graph) in the given graph starting from the specified root vertex.
     *
     * @param graph The adjacency list representation of the graph.
     * @param root The root vertex to start the path search from.
     * @return A vector containing all paths as deques of strings.
     */
    std::vector<std::deque<std::string>> FindAllPathsInGraph(const AdjacencyList& graph,
                                                             const std::string& root);

    /**
     * @brief Generates a query graph based on the given triple pattern with two variables.
     *
     * @param two_variable_tp The triple pattern containing two variables.
     * @return The adjacency list representation of the generated query graph.
     */
    AdjacencyList GenerateQueryGraph(TripplePattern& two_variable_tp);

    /**
     * @brief Computes the priority of variables.
     *
     * @param one_variable_tp The triple pattern containing one variable.
     * @param two_variable_tp The triple pattern containing two variables.
     * @param variable_frequency A hash map storing the frequency of each variable.
     * @return A hash map mapping variable names to their computed priority values.
     */
    phmap::flat_hash_map<std::string, uint> VariablePriority(
        TripplePattern& one_variable_tp,
        TripplePattern& two_variable_tp,
        phmap::flat_hash_map<std::string, uint>& variable_frequency);

    /**
     * @brief Finds all paths in the query graph. Using variable priority to choose the first vertex.
     *
     * @param all_paths A vector to store all discovered paths.
     * @param query_graph_ud The undirected query graph represented as an adjacency list.
     * @param variable_priority A hash map mapping variable names to their priority values.
     */
    void FindAllPaths(std::vector<std::deque<std::string>>& all_paths,
                      AdjacencyList& query_graph_ud,
                      phmap::flat_hash_map<std::string, uint>& variable_priority);

    /**
     * @brief Sorts variables based on their position in the paths and their priority.
     *
     * @param all_paths A vector of all paths represented as deques of strings.
     * @param variable_priority A hash map mapping variable names to their priority values.
     * @return A vector of sorted variable names based on the paths.
     */
    std::vector<std::string> PathBasedSort(std::vector<std::deque<std::string>> all_paths,
                                           phmap::flat_hash_map<std::string, uint> variable_priority);

    /**
     * @brief Insters unsorted variables into sorted variable vector.
     *
     * @param unsorted_variables A vector of unsorted variable names.
     * @param variable_priority A hash map mapping variable names to their priority values.
     */
    void HandleUnsortedVariables(std::vector<std::string>& unsorted_variables,
                                 phmap::flat_hash_map<std::string, uint> variable_priority);

    /**
     * @brief Handels variables in triple patterns with three variables.
     *
     * @param three_variable_tp The triple pattern containing three variables.
     * @param sparql_parser A shared pointer to the SPARQL parser instance.
     */
    void HandleThreeVariableTriplePattern(TripplePattern& three_variable_tp,
                                          std::shared_ptr<SPARQLParser>& sparql_parser);

    /**
     * @brief Generates the query plan table based on the given triple patterns.
     *
     * @param one_variable_tp The triple pattern containing one variable.
     * @param two_variable_tp The triple pattern containing two variables.
     * @param three_variable_tp The triple pattern containing three variables.
     */
    void GenPlanTable(TripplePattern& one_variable_tp,
                      TripplePattern& two_variable_tp,
                      TripplePattern& three_variable_tp);

   public:
    /**
     * @brief Constructs a PlanGenerator instance with the given index retriever and SPARQL parser.
     *
     * @param index A shared pointer to the IndexRetriever instance.
     * @param sparql_parser A shared pointer to the SPARQL parser instance.
     */
    PlanGenerator(std::shared_ptr<IndexRetriever>& index, std::shared_ptr<SPARQLParser>& sparql_parser);

    /**
     * @brief Maps a list of variable names to their corresponding Variable objects.
     *
     * @param variables A vector of variable names as strings.
     * @return A vector of Variable objects corresponding to the given variable names.
     */
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
