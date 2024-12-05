#include "rdf-tdaa/query/plan_generator.hpp"
#include <algorithm>
#include <unordered_set>
#include "rdf-tdaa/query/query_executor.hpp"

using Term = SPARQLParser::Term;
using Positon = Term::Positon;
using PType = PlanGenerator::Item::PType;
using RType = PlanGenerator::Item::RType;

PlanGenerator::Item::Item()
    : retrieval_type(RType::kNone),
      prestore_type(PType::kEmpty),
      index_result(),
      triple_pattern_id(0),
      search_id(0),
      father_item_id(0),
      empty_item_level(0) {}

PlanGenerator::Item::Item(const Item& other)
    : retrieval_type(other.retrieval_type),
      prestore_type(other.prestore_type),
      index_result(other.index_result),
      triple_pattern_id(other.triple_pattern_id),
      search_id(other.search_id),
      father_item_id(other.father_item_id),
      empty_item_level(other.empty_item_level) {}

PlanGenerator::Item& PlanGenerator::Item::operator=(const Item& other) {
    if (this != &other) {
        retrieval_type = other.retrieval_type;
        prestore_type = other.prestore_type;
        index_result = other.index_result;
        triple_pattern_id = other.triple_pattern_id;
        search_id = other.search_id;
        father_item_id = other.father_item_id;
        empty_item_level = other.empty_item_level;
    }
    return *this;
}

PlanGenerator::Variable::Variable() : value(""), count(0) {}

PlanGenerator::Variable::Variable(std::string value) : value(value), count(1) {}

PlanGenerator::Variable::Variable(std::string value, ulong priority, SPARQLParser::Term::Positon position)
    : value(value), priority(priority), count(1), position(position) {}

PlanGenerator::Variable& PlanGenerator::Variable::operator=(const Variable& other) {
    if (this != &other) {
        value = other.value;
        priority = other.priority;
        count = other.count;
        position = other.position;
    }
    return *this;
}

PlanGenerator::PlanGenerator(std::shared_ptr<IndexRetriever>& index,
                             std::shared_ptr<SPARQLParser>& sparql_parser)
    : index_(index) {
    const std::vector<SPARQLParser::TriplePattern>& triple_partterns = sparql_parser->TriplePatterns();

    std::vector<std::string> unsorted_variables;
    TripplePattern one_variable_tp;
    TripplePattern two_variable_tp;
    TripplePattern three_variable_tp;
    phmap::flat_hash_map<std::string, uint> variable_frequency;
    uint tp_id = 0;
    for (const auto& triple_parttern : triple_partterns) {
        auto& s = triple_parttern.subject;
        auto& p = triple_parttern.predicate;
        auto& o = triple_parttern.object;

        if (s.IsVariable())
            variable_frequency[s.value]++;
        if (o.IsVariable())
            variable_frequency[o.value]++;
        if (p.IsVariable()) {
            variable_frequency[p.value]++;
        } else {
            if (index_->Term2ID(p) == 0) {
                zero_result_ = true;
                return;
            }
        }

        if (triple_parttern.variable_cnt == 1) {
            one_variable_tp.push_back({{s, p, o}, tp_id});
            if (s.IsVariable())
                unsorted_variables.push_back(s.value);
            if (p.IsVariable())
                unsorted_variables.push_back(p.value);
            if (o.IsVariable())
                unsorted_variables.push_back(o.value);
        }
        if (triple_parttern.variable_cnt == 2)
            two_variable_tp.push_back({{s, p, o}, tp_id});
        if (triple_parttern.variable_cnt == 3)
            three_variable_tp.push_back({{s, p, o}, tp_id});
        tp_id++;
    }

    AdjacencyList query_graph_ud = GenerateQueryGraph(two_variable_tp);
    if (zero_result_)
        return;

    phmap::flat_hash_map<std::string, uint> variable_priority =
        VariablePriority(one_variable_tp, two_variable_tp, variable_frequency);
    if (zero_result_ == true)
        return;

    std::vector<std::deque<std::string>> all_paths;
    FindAllPaths(all_paths, query_graph_ud, variable_priority);

    if (debug_) {
        std::cout << "pathes: " << std::endl;
        for (auto& path : all_paths) {
            for (auto it = path.begin(); it != path.end(); it++)
                std::cout << *it << " ";
            std::cout << std::endl;
        }
        std::cout << "--------------------" << std::endl;
    }

    std::vector<std::string> ends = PathBasedSort(all_paths, variable_priority);
    std::unordered_set<std::string> existing_elements(unsorted_variables.begin(), unsorted_variables.end());
    for (const auto& variable : ends) {
        if (existing_elements.find(variable) == existing_elements.end()) {
            unsorted_variables.push_back(variable);
            existing_elements.insert(variable);
        }
    }

    HandleUnsortedVariables(unsorted_variables, variable_priority);
    HandleThreeVariableTriplePattern(three_variable_tp, sparql_parser);

    // {"?x1", "?x2", "?x3", "?x4"}; 714.31     938.473     1105.34
    // {"?x1", "?x2", "?x4", "?x3"}; 802.362    944.436     1079.22
    // {"?x1", "?x3", "?x2", "?x4"}; 829.604    983.058     3239.88
    // {"?x1", "?x3", "?x4", "?x2"}; 820.904    1153.37     34665.2
    // {"?x1", "?x4", "?x2", "?x3"}; 1027.75    3606.71     36626.1
    // {"?x1", "?x4", "?x3", "?x2"}; 1052.22    3596.07     39528.2
    // {"?x2", "?x1", "?x3", "?x4"}; 240.331    718.209     279.715
    // {"?x2", "?x1", "?x4", "?x3"}; 369.768    731.068     284.544
    // {"?x2", "?x3", "?x1", "?x4"}; 432.42     4608.49     2095.59
    // {"?x2", "?x3", "?x4", "?x1"}; 444.527    4595.26     2084.04
    // {"?x2", "?x4", "?x1", "?x3"}; 381.925    700.808     286.831
    // {"?x2", "?x4", "?x3", "?x1"}; 393.454    622.86      464.586
    // {"?x3", "?x1", "?x2", "?x4"}; 991.309    980.657     1299.33
    // {"?x3", "?x1", "?x4", "?x2"}; 983.728    962.769     25501
    // {"?x3", "?x2", "?x1", "?x4"}; 325.604    1136.82     230.804
    // {"?x3", "?x2", "?x4", "?x1"}; 318.746    1138.61     229.426
    // {"?x3", "?x4", "?x1", "?x2"}; 843.942    896.594     87584.3
    // {"?x3", "?x4", "?x2", "?x1"}; 769.821    924.417     1216.9
    // {"?x4", "?x1", "?x2", "?x3"}; 1168.3     3690.63     36560.3
    // {"?x4", "?x1", "?x3", "?x2"}; 1163.86    3655.02     42505.9
    // {"?x4", "?x2", "?x1", "?x3"}; 273.723    627.882     1056.32
    // {"?x4", "?x2", "?x3", "?x1"}; 269.347    607.544     1208.17
    // {"?x4", "?x3", "?x1", "?x2"}; 1060.76    466.864     108570
    // {"?x4", "?x3", "?x2", "?x1"}; 234.78     557.053     10196.3

    for (size_t i = 0; i < variable_order_.size(); ++i) {
        variable_order_[i].priority = i;
        value2variable_[variable_order_[i].value] = &variable_order_[i];
    }

    if (debug_) {
        std::cout << "variables order: " << std::endl;
        for (auto it = variable_order_.begin(); it != variable_order_.end(); it++)
            std::cout << it->value << " ";
        std::cout << std::endl;
        std::cout << "------------------------------" << std::endl;
    }

    GenPlanTable(one_variable_tp, two_variable_tp, three_variable_tp);

    if (debug_) {
        std::cout << "query plan: " << std::endl;
        for (auto& item : query_plan_) {
            for (auto& i : item) {
                std::cout << i.retrieval_type << " ";
            }
            std::cout << std::endl;
        }
        std::cout << "------------------------------" << std::endl;
    }
}

void PlanGenerator::DFS(const AdjacencyList& graph,
                        std::string vertex,
                        hash_map<std::string, bool>& visited,
                        AdjacencyList& tree,
                        std::deque<std::string>& current_path,
                        std::vector<std::deque<std::string>>& all_paths) {
    current_path.push_back(vertex);  // Add the current vertex to the path

    bool all_visited = true;
    for (const auto& edge : graph.at(vertex)) {
        if (!visited[edge.first])
            all_visited = false;
    }
    if (all_visited)
        all_paths.push_back(current_path);
    visited[vertex] = true;

    // Explore the adjacent vertices
    for (const auto& edge : graph.at(vertex)) {
        std::string adjVertex = edge.first;
        if (!visited[adjVertex]) {
            tree[vertex].emplace_back(adjVertex, edge.second);              // Add edge to the spanning tree
            DFS(graph, adjVertex, visited, tree, current_path, all_paths);  // Continue DFS
        }
    }

    // Backtrack: remove the current vertex from the path
    current_path.pop_back();
}

std::vector<std::deque<std::string>> PlanGenerator::FindAllPathsInGraph(const AdjacencyList& graph,
                                                                        const std::string& root) {
    hash_map<std::string, bool> visited;             // Track visited vertices
    AdjacencyList tree;                              // The resulting spanning tree
    std::deque<std::string> current_path;            // Current path from the root to the current vertex
    std::vector<std::deque<std::string>> all_paths;  // All paths from the root to the leaves

    // Initialize visited map
    for (const auto& pair : graph) {
        visited[pair.first] = false;
    }

    // Perform DFS to fill the spanning tree and find all paths
    DFS(graph, root, visited, tree, current_path, all_paths);

    return all_paths;
}

AdjacencyList PlanGenerator::GenerateQueryGraph(TripplePattern& two_variable_tp) {
    AdjacencyList query_graph_ud;

    for (const auto& tp : two_variable_tp) {
        auto& [s, p, o] = tp.first;

        std::string v_value_1;
        std::string v_value_2;
        uint edge = 0;

        if (s.IsVariable() && !p.IsVariable() && o.IsVariable()) {
            v_value_1 = s.value;
            v_value_2 = o.value;
            edge = index_->Term2ID(p);
        }
        if (!s.IsVariable() && p.IsVariable() && o.IsVariable()) {
            v_value_1 = p.value;
            v_value_2 = o.value;
            edge = index_->Term2ID(s);
        }
        if (s.IsVariable() && p.IsVariable() && !o.IsVariable()) {
            v_value_1 = s.value;
            v_value_2 = p.value;
            edge = index_->Term2ID(o);
        }
        query_graph_ud[v_value_1].push_back({v_value_2, edge});
        query_graph_ud[v_value_2].push_back({v_value_1, edge});
    }

    return query_graph_ud;
}

phmap::flat_hash_map<std::string, uint> PlanGenerator::VariablePriority(
    TripplePattern& one_variable_tp,
    TripplePattern& two_variable_tp,
    phmap::flat_hash_map<std::string, uint>& variable_frequency) {
    uint max_frequency = 0;
    for (const auto& [variable, frequency] : variable_frequency) {
        if (frequency > max_frequency)
            max_frequency = frequency;
    }
    std::set<std::string> max_frequency_variables;
    for (const auto& [variable, frequency] : variable_frequency) {
        if (frequency == max_frequency)
            max_frequency_variables.insert(variable);
    }

    if (max_frequency_variables.size() == 1)
        max_frequency_variables.clear();

    phmap::flat_hash_map<std::string, std::vector<std::span<uint>>> variable_candidates;
    phmap::flat_hash_map<std::string, std::vector<uint>> variable_cardinality;
    for (const auto& tp : one_variable_tp) {
        auto& [s, p, o] = tp.first;
        std::string v_value;
        uint size = 0;
        std::span<uint> candidates;
        if (s.IsVariable() && !p.IsVariable() && !o.IsVariable()) {
            v_value = s.value;
            if (max_frequency_variables.contains(v_value))
                candidates = index_->GetByOP(index_->Term2ID(o), index_->Term2ID(p));
            else
                size = index_->GetByOPSize(index_->Term2ID(o), index_->Term2ID(p));
        }
        if (!s.IsVariable() && p.IsVariable() && !o.IsVariable()) {
            v_value = p.value;
            if (max_frequency_variables.contains(v_value))
                candidates = index_->GetBySO(index_->Term2ID(s), index_->Term2ID(o));
            else
                size = index_->GetBySOSize(index_->Term2ID(s), index_->Term2ID(o));
        }
        if (!s.IsVariable() && !p.IsVariable() && o.IsVariable()) {
            v_value = o.value;
            if (max_frequency_variables.contains(v_value))
                candidates = index_->GetBySP(index_->Term2ID(s), index_->Term2ID(p));
            else
                size = index_->GetBySPSize(index_->Term2ID(s), index_->Term2ID(p));
        }
        if (size == 0 && candidates.empty()) {
            zero_result_ = true;
            return {};
        }
        if (size != 0)
            variable_cardinality[v_value].push_back(size);
        if (!candidates.empty())
            variable_candidates[v_value].push_back(candidates);
    }
    for (const auto& tp : two_variable_tp) {
        auto& [s, p, o] = tp.first;
        std::string v_value_1;
        std::string v_value_2;
        uint edge = 0;
        uint size1 = 0;
        uint size2 = 0;
        std::span<uint> candidates1;
        std::span<uint> candidates2;
        if (s.IsVariable() && !p.IsVariable() && o.IsVariable()) {
            v_value_1 = s.value;
            v_value_2 = o.value;
            edge = index_->Term2ID(p);
            if (max_frequency_variables.contains(v_value_1))
                candidates1 = index_->GetSSet(edge);
            else
                size1 = index_->GetSSetSize(edge);
            if (max_frequency_variables.contains(v_value_2))
                candidates2 = index_->GetOSet(edge);
            else
                size2 = index_->GetOSetSize(edge);
        }
        if (!s.IsVariable() && p.IsVariable() && o.IsVariable()) {
            v_value_1 = p.value;
            v_value_2 = o.value;
            edge = index_->Term2ID(s);
            if (max_frequency_variables.contains(v_value_1))
                candidates1 = index_->GetSPreSet(edge);
            else
                size1 = index_->GetSPreSet(edge).size();
            if (max_frequency_variables.contains(v_value_2))
                candidates2 = index_->GetByS(edge);
            else
                size2 = index_->GetBySSize(edge);
        }
        if (s.IsVariable() && p.IsVariable() && !o.IsVariable()) {
            v_value_1 = s.value;
            v_value_2 = p.value;
            edge = index_->Term2ID(o);
            if (max_frequency_variables.contains(v_value_1))
                candidates1 = index_->GetByO(edge);
            else
                size1 = index_->GetByOSize(edge);
            if (max_frequency_variables.contains(v_value_2))
                candidates2 = index_->GetOPreSet(edge);
            else
                size2 = index_->GetOPreSet(edge).size();
        }
        if (edge == 0) {
            zero_result_ = true;
            return {};
        }
        if (size1 != 0)
            variable_cardinality[v_value_1].push_back(size1);
        if (!candidates1.empty())
            variable_candidates[v_value_1].push_back(candidates1);
        if (size2 != 0)
            variable_cardinality[v_value_2].push_back(size2);
        if (!candidates2.empty())
            variable_candidates[v_value_2].push_back(candidates2);
    }
    std::vector<std::string> variable_sort;
    phmap::flat_hash_map<std::string, uint> est_size;

    for (const auto& [v_value, sizes] : variable_cardinality) {
        est_size[v_value] = *std::min_element(sizes.begin(), sizes.end());
        variable_sort.push_back(v_value);
    }
    for (const auto& [v_value, candidates] : variable_candidates) {
        est_size[v_value] = QueryExecutor::LeapfrogJoin(candidates).size();
        variable_sort.push_back(v_value);
    }
    // phmap::flat_hash_map<std::string, std::vector<uint>> variable_cardinality;
    // for (const auto& tp : one_variable_tp) {
    //     auto& [s, p, o] = tp.first;
    //     std::string v_value;
    //     uint size = 0;
    //     if (s.IsVariable() && !p.IsVariable() && !o.IsVariable()) {
    //         v_value = s.value;
    //         size = index_->GetByOPSize(index_->Term2ID(o), index_->Term2ID(p));
    //     }
    //     if (!s.IsVariable() && p.IsVariable() && !o.IsVariable()) {
    //         v_value = p.value;
    //         size = index_->GetBySOSize(index_->Term2ID(s), index_->Term2ID(o));
    //     }
    //     if (!s.IsVariable() && !p.IsVariable() && o.IsVariable()) {
    //         v_value = o.value;
    //         size = index_->GetBySPSize(index_->Term2ID(s), index_->Term2ID(p));
    //     }
    //     if (size == 0) {
    //         zero_result_ = true;
    //         return {};
    //     }
    //     variable_cardinality[v_value].push_back(size);
    // }
    // for (const auto& tp : two_variable_tp) {
    //     auto& [s, p, o] = tp.first;
    //     std::string v_value_1;
    //     std::string v_value_2;
    //     uint edge = 0;
    //     uint size1 = 0;
    //     uint size2 = 0;
    //     if (s.IsVariable() && !p.IsVariable() && o.IsVariable()) {
    //         v_value_1 = s.value;
    //         v_value_2 = o.value;
    //         edge = index_->Term2ID(p);
    //         size1 = index_->GetSSetSize(edge);
    //         size2 = index_->GetOSetSize(edge);
    //     }
    //     if (!s.IsVariable() && p.IsVariable() && o.IsVariable()) {
    //         v_value_1 = p.value;
    //         v_value_2 = o.value;
    //         edge = index_->Term2ID(s);
    //         size1 = index_->GetSPreSet(edge).size();
    //         size2 = index_->GetBySSize(edge);
    //     }
    //     if (s.IsVariable() && p.IsVariable() && !o.IsVariable()) {
    //         v_value_1 = s.value;
    //         v_value_2 = p.value;
    //         edge = index_->Term2ID(o);
    //         size1 = index_->GetByOSize(edge);
    //         size2 = index_->GetOPreSet(edge).size();
    //     }
    //     if (edge == 0) {
    //         zero_result_ = true;
    //         return {};
    //     }
    //     variable_cardinality[v_value_1].push_back(size1);
    //     variable_cardinality[v_value_2].push_back(size2);
    // }
    // std::vector<std::string> variable_sort;
    // phmap::flat_hash_map<std::string, uint> est_size;
    // for (const auto& [v_value, sizes] : variable_cardinality) {
    //     est_size[v_value] = *std::min_element(sizes.begin(), sizes.end());
    //     variable_sort.push_back(v_value);
    // }

    // phmap::flat_hash_map<std::string, std::vector<std::span<uint>>> variable_candidates;
    // for (const auto& tp : one_variable_tp) {
    //     auto& [s, p, o] = tp.first;
    //     std::string v_value;
    //     std::span<uint> candidates;
    //     if (s.IsVariable() && !p.IsVariable() && !o.IsVariable()) {
    //         v_value = s.value;
    //         candidates = index_->GetByOP(index_->Term2ID(o), index_->Term2ID(p));
    //     }
    //     if (!s.IsVariable() && p.IsVariable() && !o.IsVariable()) {
    //         v_value = p.value;
    //         candidates = index_->GetBySO(index_->Term2ID(s), index_->Term2ID(o));
    //     }
    //     if (!s.IsVariable() && !p.IsVariable() && o.IsVariable()) {
    //         v_value = o.value;
    //         candidates = index_->GetBySP(index_->Term2ID(s), index_->Term2ID(p));
    //     }
    //     if (candidates.size() == 0) {
    //         zero_result_ = true;
    //         return {};
    //     }
    //     variable_candidates[v_value].push_back(candidates);
    // }
    // for (const auto& tp : two_variable_tp) {
    //     auto& [s, p, o] = tp.first;
    //     std::string v_value_1;
    //     std::string v_value_2;
    //     uint edge = 0;
    //     std::span<uint> candidates1;
    //     std::span<uint> candidates2;
    //     if (s.IsVariable() && !p.IsVariable() && o.IsVariable()) {
    //         v_value_1 = s.value;
    //         v_value_2 = o.value;
    //         edge = index_->Term2ID(p);
    //         candidates1 = index_->GetSSet(edge);
    //         candidates2 = index_->GetOSet(edge);
    //     }
    //     if (!s.IsVariable() && p.IsVariable() && o.IsVariable()) {
    //         v_value_1 = p.value;
    //         v_value_2 = o.value;
    //         edge = index_->Term2ID(s);
    //         candidates1 = index_->GetSPreSet(edge);
    //         candidates2 = index_->GetByS(edge);
    //     }
    //     if (s.IsVariable() && p.IsVariable() && !o.IsVariable()) {
    //         v_value_1 = s.value;
    //         v_value_2 = p.value;
    //         edge = index_->Term2ID(o);
    //         candidates1 = index_->GetByO(edge);
    //         candidates2 = index_->GetOPreSet(edge);
    //     }
    //     if (edge == 0) {
    //         zero_result_ = true;
    //         return {};
    //     }
    //     variable_candidates[v_value_1].push_back(candidates1);
    //     variable_candidates[v_value_2].push_back(candidates2);
    // }
    // std::vector<std::string> variable_sort;
    // phmap::flat_hash_map<std::string, uint> est_size;
    // for (const auto& [v_value, candidates] : variable_candidates) {
    //     est_size[v_value] = QueryExecutor::LeapfrogJoin(candidates).size();
    //     variable_sort.push_back(v_value);
    // }

    phmap::flat_hash_map<std::string, uint> variable_priority;

    std::sort(variable_sort.begin(), variable_sort.end(), [&](const auto& var1, const auto& var2) {
        if (variable_frequency[var1] != variable_frequency[var2]) {
            return variable_frequency[var1] > variable_frequency[var2];
        }
        return est_size[var1] < est_size[var2];
    });

    for (size_t i = 0; i < variable_sort.size(); i++)
        variable_priority[variable_sort[i]] = i;

    if (debug_) {
        std::cout << "------------------------------" << std::endl;
        for (auto& v : variable_sort)
            std::cout << v << ": " << variable_frequency[v] << " " << est_size[v] << std::endl;
    }

    return variable_priority;
}

void PlanGenerator::FindAllPaths(std::vector<std::deque<std::string>>& all_paths,
                                 AdjacencyList& query_graph_ud,
                                 phmap::flat_hash_map<std::string, uint>& variable_priority) {
    std::vector<std::string> variable_order(query_graph_ud.size());
    std::transform(query_graph_ud.begin(), query_graph_ud.end(), variable_order.begin(),
                   [](const auto& pair) { return pair.first; });
    std::sort(variable_order.begin(), variable_order.end(), [&](const auto& var1, const auto& var2) {
        return variable_priority[var1] < variable_priority[var2];
    });

    for (auto vertex_it = query_graph_ud.begin(); vertex_it != query_graph_ud.end(); vertex_it++) {
        std::sort(vertex_it->second.begin(), vertex_it->second.end(),
                  [&](const auto& edge1, const auto& edge2) {
                      return variable_priority[edge1.first] < variable_priority[edge2.first];
                  });
    }

    if (debug_) {
        for (auto vertex_it = query_graph_ud.begin(); vertex_it != query_graph_ud.end(); vertex_it++) {
            std::cout << vertex_it->first << ": ";
            for (auto& edge : vertex_it->second)
                std::cout << " (" << edge.first << "," << edge.second << ") ";
            std::cout << std::endl;
        }
    }

    std::vector<std::deque<std::string>> partial_paths;
    if (query_graph_ud.size() != 0) {
        // 考虑非连通图
        while (variable_order.size() > 0) {
            partial_paths = FindAllPathsInGraph(query_graph_ud, variable_order[0]);
            for (auto& path : partial_paths) {
                std::unordered_set<std::string> path_set(path.begin(), path.end());
                variable_order.erase(
                    std::remove_if(variable_order.begin(), variable_order.end(),
                                   [&path_set](std::string v) { return path_set.contains(v); }),
                    variable_order.end());
                all_paths.push_back(path);
            }
        }
    }
}

std::vector<std::string> PlanGenerator::PathBasedSort(
    std::vector<std::deque<std::string>> all_paths,
    phmap::flat_hash_map<std::string, uint> variable_priority) {
    std::vector<std::string> ends;
    while (!all_paths.empty()) {
        std::string higher_priority_variable;
        size_t higher_priority = std::numeric_limits<size_t>::max();

        for (const auto& path : all_paths) {
            if (path.size() > 1) {
                size_t current_priority = variable_priority[path.front()];
                if (current_priority < higher_priority) {
                    higher_priority = current_priority;
                    higher_priority_variable = path.front();
                }
            }
        }

        if (!higher_priority_variable.empty())
            variable_order_.push_back(higher_priority_variable);

        for (auto it = all_paths.begin(); it != all_paths.end();) {
            if (it->size() == 1) {
                ends.push_back(it->back());
                it->pop_front();
            }
            if (!it->empty() && it->front() == higher_priority_variable)
                it->pop_front();

            it = (it->empty()) ? all_paths.erase(it) : it + 1;
        }
    }

    std::sort(ends.begin(), ends.end(), [&](const auto& var1, const auto& var2) {
        return variable_priority[var1] < variable_priority[var2];
    });

    return ends;
}

void PlanGenerator::HandleUnsortedVariables(std::vector<std::string>& unsorted_variables,
                                            phmap::flat_hash_map<std::string, uint> variable_priority) {
    std::sort(unsorted_variables.begin(), unsorted_variables.end(), [&](const auto& var1, const auto& var2) {
        return variable_priority[var1] < variable_priority[var2];
    });

    for (auto it = unsorted_variables.begin(); it != unsorted_variables.end(); it++) {
        bool contains = false;
        for (auto& v : variable_order_) {
            if (v.value == *it)
                contains = true;
        }
        if (!contains)
            variable_order_.push_back(*it);
    }
}

void PlanGenerator::HandleThreeVariableTriplePattern(TripplePattern& three_variable_tp,
                                                     std::shared_ptr<SPARQLParser>& sparql_parser) {
    for (const auto& tp : three_variable_tp) {
        auto& [s, p, o] = tp.first;

        bool s_contains = false, o_contains = false;
        uint offset = 0;
        for (uint i = 0; i < variable_order_.size(); i++) {
            if (variable_order_[i].value == s.value) {
                s_contains = true;
                if (offset == 0)
                    offset = i;
            }
            if (variable_order_[i].value == o.value) {
                o_contains = true;
                if (offset == 0)
                    offset = i;
            }
        }
        if (s_contains && o_contains) {
            variable_order_.insert(variable_order_.begin() + offset, p.value);
        } else {
            variable_order_.push_back(p.value);
            distinct_predicate_ = false;
            if (sparql_parser->project_modifier().modifier_type == SPARQLParser::ProjectModifier::Distinct) {
                auto& variables = sparql_parser->ProjectVariables();
                if (variables.size() == 1) {
                    if (p.value == variables[0])
                        distinct_predicate_ = true;
                }
            }
            if (s_contains && !o_contains && !distinct_predicate_)
                variable_order_.push_back(o.value);
            if (!s_contains && o_contains && !distinct_predicate_)
                variable_order_.push_back(s.value);
        }
    }
}

void PlanGenerator::GenPlanTable(TripplePattern& one_variable_tp,
                                 TripplePattern& two_variable_tp,
                                 TripplePattern& three_variable_tp) {
    size_t n = variable_order_.size();
    query_plan_.resize(n);
    pre_results_.resize(n);
    filled_item_indices_.resize(n);
    empty_item_indices_.resize(n);

    for (const auto& tp : one_variable_tp) {
        auto& [s, p, o] = tp.first;
        uint s_var_id = 0;
        uint p_var_id = 0;
        uint o_var_id = 0;

        if (s.IsVariable() && !p.IsVariable() && !o.IsVariable()) {
            s_var_id = value2variable_[s.value]->priority;
            value2variable_[s.value]->position = Term::Positon::kSubject;
            uint oid = index_->Term2ID(o);
            uint pid = index_->Term2ID(p);
            std::span<uint> r = index_->GetByOP(oid, pid);
            pre_results_[s_var_id].push_back(r);
        }
        if (!s.IsVariable() && p.IsVariable() && !o.IsVariable()) {
            p_var_id = value2variable_[p.value]->priority;
            value2variable_[p.value]->position = Term::Positon::kPredicate;
            uint sid = index_->Term2ID(s);
            uint oid = index_->Term2ID(o);
            std::span<uint> r = index_->GetBySO(sid, oid);
            pre_results_[p_var_id].push_back(r);
        }
        if (!s.IsVariable() && !p.IsVariable() && o.IsVariable()) {
            o_var_id = value2variable_[o.value]->priority;
            value2variable_[o.value]->position = Term::Positon::kObject;
            uint sid = index_->Term2ID(s);
            uint pid = index_->Term2ID(p);
            std::span<uint> r = index_->GetBySP(sid, pid);
            pre_results_[o_var_id].push_back(r);
        }
    }

    for (const auto& tp : two_variable_tp) {
        auto& [s, p, o] = tp.first;
        Item filled_item, empty_item;
        uint first_priority, second_priority;
        bool is_first_prior = false;

        auto process_filled_item = [&](const Term& fixed_term, const Term& var_term1, const Term& var_term2,
                                       Positon var1_position, Positon var2_position, PType prestore_type1,
                                       PType prestore_type2, RType retrieval_type1, RType retrieval_type2,
                                       auto index_func1, auto index_func2) {
            value2variable_[var_term1.value]->position = var1_position;
            value2variable_[var_term2.value]->position = var2_position;

            first_priority = value2variable_[var_term1.value]->priority;
            second_priority = value2variable_[var_term2.value]->priority;

            is_first_prior = first_priority < second_priority;

            filled_item.search_id = index_->Term2ID(fixed_term);
            filled_item.prestore_type = is_first_prior ? prestore_type1 : prestore_type2;
            filled_item.retrieval_type = is_first_prior ? retrieval_type1 : retrieval_type2;
            filled_item.index_result = is_first_prior ? ((*index_).*index_func1)(filled_item.search_id)
                                                      : ((*index_).*index_func2)(filled_item.search_id);
        };

        if (!s.IsVariable() && p.IsVariable() && o.IsVariable()) {
            process_filled_item(s, p, o, Positon::kPredicate, Positon::kObject, PType::kPredicate,
                                PType::kObject, RType::kGetBySP, RType::kGetBySO, &IndexRetriever::GetSPreSet,
                                &IndexRetriever::GetByS);
        } else if (s.IsVariable() && !p.IsVariable() && o.IsVariable()) {
            process_filled_item(p, s, o, Positon::kSubject, Positon::kObject, PType::kPreSub, PType::kPreObj,
                                RType::kGetBySP, RType::kGetByOP, &IndexRetriever::GetSSet,
                                &IndexRetriever::GetOSet);
        } else if (s.IsVariable() && p.IsVariable() && !o.IsVariable()) {
            process_filled_item(o, s, p, Positon::kSubject, Positon::kPredicate, PType::kSubject,
                                PType::kPredicate, RType::kGetBySO, RType::kGetByOP, &IndexRetriever::GetByO,
                                &IndexRetriever::GetOPreSet);
        }

        uint higher_priority = is_first_prior ? first_priority : second_priority;
        uint lower_priority = is_first_prior ? second_priority : first_priority;

        filled_item.triple_pattern_id = tp.second;
        filled_item.empty_item_level = lower_priority;

        empty_item.search_id = filled_item.search_id;
        empty_item.prestore_type = Item::PType::kEmpty;
        empty_item.retrieval_type = Item::RType::kNone;
        empty_item.index_result = std::span<uint>();
        empty_item.triple_pattern_id = tp.second;
        empty_item.empty_item_level = 0;

        query_plan_[higher_priority].push_back(filled_item);
        filled_item_indices_[higher_priority].push_back(query_plan_[higher_priority].size() - 1);
        query_plan_[lower_priority].push_back(empty_item);
        empty_item_indices_[lower_priority].push_back(query_plan_[lower_priority].size() - 1);
    }

    for (const auto& tp : three_variable_tp) {
        auto& [s, p, o] = tp.first;
        value2variable_[s.value]->position = Term::Positon::kSubject;
        value2variable_[p.value]->position = Term::Positon::kPredicate;
        std::vector<std::pair<Positon, uint>> pattern_variable_level = {
            {Positon::kSubject, value2variable_[s.value]->priority},
            {Positon::kPredicate, value2variable_[p.value]->priority}};
        if (!distinct_predicate_) {
            value2variable_[o.value]->position = Term::Positon::kObject;
            pattern_variable_level.push_back({Positon::kObject, value2variable_[o.value]->priority});
        }

        std::sort(pattern_variable_level.begin(), pattern_variable_level.end(),
                  [&](const auto& var1, const auto& var2) { return var1.second < var2.second; });
        uint max = (distinct_predicate_) ? 2 : 3;
        for (uint i = 0; i < max; i++) {
            Item empty_item;
            if (i == 0) {
                if (pattern_variable_level[0].first == Positon::kSubject &&
                    pattern_variable_level[1].first == Positon::kPredicate) {
                    empty_item.retrieval_type = Item::RType::kGetSPreSet;
                }
                if (pattern_variable_level[0].first == Positon::kObject &&
                    pattern_variable_level[1].first == Positon::kPredicate) {
                    empty_item.retrieval_type = Item::RType::kGetOPreSet;
                }
                empty_item.empty_item_level = pattern_variable_level[1].second;
            }
            if (i == 1) {
                if (pattern_variable_level[1].first == Positon::kPredicate &&
                    pattern_variable_level[2].first == Positon::kObject) {
                    empty_item.retrieval_type = Item::RType::kGetBySP;
                }
                if (pattern_variable_level[1].first == Positon::kPredicate &&
                    pattern_variable_level[2].first == Positon::kSubject) {
                    empty_item.retrieval_type = Item::RType::kGetByOP;
                }
                empty_item.father_item_id = pattern_variable_level[0].second;
                empty_item.empty_item_level = (distinct_predicate_) ? 0 : pattern_variable_level[2].second;
            }
            if (i == 2) {
                empty_item.retrieval_type = Item::RType::kNone;
                empty_item.empty_item_level = 0;
            }

            empty_item.search_id = 0;
            empty_item.prestore_type = Item::PType::kEmpty;
            empty_item.index_result = std::span<uint>();
            empty_item.triple_pattern_id = tp.second;
            uint level = pattern_variable_level[i].second;
            query_plan_[level].push_back(empty_item);
            empty_item_indices_[level].push_back(query_plan_[level].size() - 1);
        }
    }
}

std::vector<PlanGenerator::Variable> PlanGenerator::MappingVariable(
    const std::vector<std::string>& variables) {
    std::vector<Variable> ret;
    ret.reserve(variables.size());
    for (const auto& var : variables)
        ret.push_back(*value2variable_.at(var));
    return ret;
}

std::vector<std::vector<PlanGenerator::Item>>& PlanGenerator::query_plan() {
    return query_plan_;
}

hash_map<std::string, PlanGenerator::Variable*>& PlanGenerator::value2variable() {
    return value2variable_;
}

std::vector<std::vector<uint>>& PlanGenerator::filled_item_indices() {
    return filled_item_indices_;
}

std::vector<std::vector<uint>>& PlanGenerator::empty_item_indices() {
    return empty_item_indices_;
}

std::vector<std::vector<std::span<uint>>>& PlanGenerator::pre_results() {
    return pre_results_;
}

bool PlanGenerator::zero_result() {
    return zero_result_;
}

bool PlanGenerator::distinct_predicate() {
    return distinct_predicate_;
}