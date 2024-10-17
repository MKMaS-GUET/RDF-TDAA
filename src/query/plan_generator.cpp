#include "rdf-tdaa/query/plan_generator.hpp"

using Term = SPARQLParser::Term;
using Positon = Term::Positon;
using PType = PlanGenerator::Item::PType;
using RType = PlanGenerator::Item::RType;

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
    : index_(index), sparql_parser_(sparql_parser) {
    Generate();
}

void PlanGenerator::DFS(const AdjacencyList& graph,
                        std::string vertex,
                        hash_map<std::string, bool>& visited,
                        AdjacencyList& tree,
                        std::deque<std::string>& current_path,
                        std::vector<std::deque<std::string>>& all_paths) {
    current_path.push_back(vertex);  // Add the current vertex to the path

    // Check if it's a leaf node in the spanning tree (no adjacent vertices)
    // if (graph.at(vertex).size() == 1 || visited[vertex] == true) {
    //     all_paths.push_back(current_path);  // Save the current path if it's a leaf
    // }
    bool all_visited = true;
    for (const auto& edge : graph.at(vertex)) {
        if (!visited[edge.first]) {
            all_visited = false;
        }
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

void PlanGenerator::Generate() {
    AdjacencyList query_graph_ud;
    hash_map<std::string, uint> est_size;
    hash_map<std::string, Variable> univariates;

    const std::vector<SPARQLParser::TriplePattern>& triple_partterns = sparql_parser_->TriplePatterns();

    for (const auto& triple_parttern : triple_partterns) {
        auto& s = triple_parttern.subject;
        auto& p = triple_parttern.predicate;
        auto& o = triple_parttern.object;

        // one variable
        if (triple_parttern.variable_cnt == 1) {
            uint size = 0;
            std::string v_value;

            if (s.IsVariable() && !p.IsVariable() && !o.IsVariable()) {
                v_value = s.value;
                size = index_->GetByOPSize(index_->Term2ID(o), index_->Term2ID(p));
            }
            if (!s.IsVariable() && p.IsVariable() && !o.IsVariable()) {
                v_value = p.value;
                size = index_->GetBySOSize(index_->Term2ID(s), index_->Term2ID(o));
            }
            if (!s.IsVariable() && !p.IsVariable() && o.IsVariable()) {
                v_value = o.value;
                size = index_->GetBySPSize(index_->Term2ID(s), index_->Term2ID(p));
            }
            if (size == 0) {
                zero_result_ = true;
                return;
            }
            if (est_size[v_value] == 0 || est_size[v_value] > size)
                est_size[v_value] = size;
            auto it = univariates.find(v_value);
            if (it != univariates.end()) {
                it->second.count++;
            } else {
                univariates.insert({v_value, Variable(v_value)});
            }
        }

        // two variables
        if (triple_parttern.variable_cnt == 2) {
            std::string v_value_1;
            std::string v_value_2;
            uint edge = 0;
            uint size1 = 0;
            uint size2 = 0;

            if (s.IsVariable() && !p.IsVariable() && o.IsVariable()) {
                v_value_1 = s.value;
                v_value_2 = o.value;
                edge = index_->Term2ID(p);
                size1 = index_->GetSSetSize(edge);
                size2 = index_->GetOSetSize(edge);
            }
            if (!s.IsVariable() && p.IsVariable() && o.IsVariable()) {
                v_value_1 = p.value;
                v_value_2 = o.value;
                edge = index_->Term2ID(s);
                size1 = index_->GetSPreSet(edge).size();
                size2 = index_->GetBySSize(edge);
            }
            if (s.IsVariable() && p.IsVariable() && !o.IsVariable()) {
                v_value_1 = s.value;
                v_value_2 = p.value;
                edge = index_->Term2ID(o);
                size1 = index_->GetByOSize(edge);
                size2 = index_->GetOPreSet(edge).size();
            }

            if (edge == 0) {
                zero_result_ = true;
                return;
            }
            query_graph_ud[v_value_1].push_back({v_value_2, edge});
            query_graph_ud[v_value_2].push_back({v_value_1, edge});
            if (est_size[v_value_1] == 0 || est_size[v_value_1] > size1)
                est_size[v_value_1] = size1;
            if (est_size[v_value_2] == 0 || est_size[v_value_2] > size2)
                est_size[v_value_2] = size2;
        }
    }

    SortVariables(query_graph_ud, univariates, est_size);

    for (const auto& triple_parttern : triple_partterns) {
        auto& s = triple_parttern.subject;
        auto& p = triple_parttern.predicate;
        auto& o = triple_parttern.object;

        if (triple_parttern.variable_cnt == 3) {
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
                if (sparql_parser_->project_modifier().modifier_type ==
                    SPARQLParser::ProjectModifier::Distinct) {
                    auto& variables = sparql_parser_->ProjectVariables();
                    if (variables.size() == 1) {
                        if (p.value == variables[0]) {
                            distinct_predicate_ = true;
                        }
                    }
                }
                if (s_contains && !o_contains && !distinct_predicate_)
                    variable_order_.push_back(o.value);
                if (!s_contains && o_contains && !distinct_predicate_)
                    variable_order_.push_back(s.value);
            }
        }
    }

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

    GenPlanTable();
}

void PlanGenerator::SortVariables(AdjacencyList& query_graph_ud,
                                  hash_map<std::string, Variable>& univariates,
                                  hash_map<std::string, uint>& est_size) {
    if (debug_) {
        for (auto vertex_it = query_graph_ud.begin(); vertex_it != query_graph_ud.end(); vertex_it++) {
            std::cout << vertex_it->first << ": ";
            for (auto& edge : vertex_it->second)
                std::cout << " (" << edge.first << "," << edge.second << ") ";
            std::cout << std::endl;
        }
        for (auto& pair : est_size)
            std::cout << pair.first << ": " << pair.second << std::endl;
    }

    // 作为第一个变量的优先级
    std::vector<std::string> variable_priority(query_graph_ud.size());
    std::transform(query_graph_ud.begin(), query_graph_ud.end(), variable_priority.begin(),
                   [](const auto& pair) { return pair.first; });

    auto sort_func = [&](const auto& var1, const auto& var2) {
        if (query_graph_ud[var1].size() + univariates[var1].count !=
            query_graph_ud[var2].size() + univariates[var2].count) {
            return query_graph_ud[var1].size() + univariates[var1].count >
                   query_graph_ud[var2].size() + univariates[var2].count;
        }
        return est_size[var1] < est_size[var2];
    };

    std::sort(variable_priority.begin(), variable_priority.end(), sort_func);

    if (debug_) {
        std::cout << "------------------------------" << std::endl;
        for (auto& v : variable_priority)
            std::cout << v << ": " << est_size[v] << std::endl;
    }

    std::vector<std::deque<std::string>> all_paths;
    std::vector<std::deque<std::string>> partial_paths;
    uint longest_path = 0;
    while (variable_priority.size() > 0) {
        partial_paths = FindAllPathsInGraph(query_graph_ud, variable_priority[0]);
        for (auto& path : partial_paths) {
            for (auto& v : path) {
                for (auto it = variable_priority.begin(); it != variable_priority.end(); it++) {
                    if (*it == v) {
                        variable_priority.erase(it);
                        break;
                    }
                }
            }
            if (all_paths.size() == 0 || path.size() > all_paths[longest_path].size())
                longest_path = all_paths.size();
            all_paths.push_back(path);
        }
    }

    if (debug_) {
        std::cout << "longest_path: " << longest_path << std::endl;
        for (auto& path : all_paths) {
            for (auto it = path.begin(); it != path.end(); it++)
                std::cout << *it << " ";
            std::cout << std::endl;
        }
        std::cout << "--------------------" << std::endl;
    }

    std::vector<std::string> ends;
    while (!all_paths.empty()) {
        std::string higher_priority_variable;
        size_t higher_priority = std::numeric_limits<size_t>::max();

        for (const auto& path : all_paths) {
            if (path.size() > 1) {
                size_t current_priority =
                    query_graph_ud[path.front()].size() + univariates[path.front()].count;
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

    std::sort(ends.begin(), ends.end(), sort_func);
    for (auto& v : ends)
        variable_order_.push_back(v);

    std::vector<std::string> univariates_order;
    for (auto it = univariates.begin(); it != univariates.end(); it++) {
        bool contains = false;
        for (auto& v : variable_order_) {
            if (v.value == it->first)
                contains = true;
        }
        if (!contains)
            univariates_order.push_back(it->first);
    }
    std::sort(univariates_order.begin(), univariates_order.end(),
              [&](const auto& var1, const auto& var2) { return est_size[var1] < est_size[var2]; });
    for (auto it = univariates_order.begin(); it != univariates_order.end(); it++)
        variable_order_.push_back(*it);
}

void PlanGenerator::GenPlanTable() {
    const std::vector<SPARQLParser::TriplePattern>& triple_partterns = sparql_parser_->TriplePatterns();

    size_t n = variable_order_.size();
    query_plan_.resize(n);
    pre_results_.resize(n);
    filled_item_indices_.resize(n);
    empty_item_indices_.resize(n);

    uint triple_pattern_id = 0;
    for (const auto& triple_parttern : triple_partterns) {
        auto& s = triple_parttern.subject;
        auto& p = triple_parttern.predicate;
        auto& o = triple_parttern.object;

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

        if (triple_parttern.variable_cnt == 2) {
            Item filled_item, empty_item;
            uint first_priority, second_priority;
            bool is_first_prior = false;

            auto process_filled_item = [&](const Term& fixed_term, const Term& var_term1,
                                           const Term& var_term2, Positon var1_position,
                                           Positon var2_position, PType prestore_type1, PType prestore_type2,
                                           RType retrieval_type1, RType retrieval_type2, auto index_func1,
                                           auto index_func2) {
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
                                    PType::kObject, RType::kGetBySP, RType::kGetBySO,
                                    &IndexRetriever::GetSPreSet, &IndexRetriever::GetByS);
            } else if (s.IsVariable() && !p.IsVariable() && o.IsVariable()) {
                process_filled_item(p, s, o, Positon::kSubject, Positon::kObject, PType::kPreSub,
                                    PType::kPreObj, RType::kGetBySP, RType::kGetByOP,
                                    &IndexRetriever::GetSSet, &IndexRetriever::GetOSet);
            } else if (s.IsVariable() && p.IsVariable() && !o.IsVariable()) {
                process_filled_item(o, s, p, Positon::kSubject, Positon::kPredicate, PType::kSubject,
                                    PType::kPredicate, RType::kGetBySO, RType::kGetByOP,
                                    &IndexRetriever::GetByO, &IndexRetriever::GetOPreSet);
            }

            uint higher_priority = is_first_prior ? first_priority : second_priority;
            uint lower_priority = is_first_prior ? second_priority : first_priority;

            filled_item.triple_pattern_id = triple_pattern_id;
            filled_item.empty_item_level = lower_priority;

            empty_item.search_id = filled_item.search_id;
            empty_item.prestore_type = Item::PType::kEmpty;
            empty_item.retrieval_type = Item::RType::kNone;
            empty_item.index_result = std::span<uint>();
            empty_item.triple_pattern_id = triple_pattern_id;
            empty_item.empty_item_level = 0;

            query_plan_[higher_priority].push_back(filled_item);
            filled_item_indices_[higher_priority].push_back(query_plan_[higher_priority].size() - 1);
            query_plan_[lower_priority].push_back(empty_item);
            empty_item_indices_[lower_priority].push_back(query_plan_[lower_priority].size() - 1);
        }

        if (triple_parttern.variable_cnt == 3) {
            value2variable_[s.value]->position = Term::Positon::kSubject;
            value2variable_[p.value]->position = Term::Positon::kPredicate;
            std::vector<std::pair<Positon, uint>> pattern_variable_order = {
                {Positon::kSubject, value2variable_[s.value]->priority},
                {Positon::kPredicate, value2variable_[p.value]->priority}};
            if (!distinct_predicate_) {
                value2variable_[o.value]->position = Term::Positon::kObject;
                pattern_variable_order.push_back({Positon::kObject, value2variable_[o.value]->priority});
            }

            std::sort(pattern_variable_order.begin(), pattern_variable_order.end(),
                      [&](const auto& var1, const auto& var2) { return var1.second < var2.second; });

            uint max = (distinct_predicate_) ? 2 : 3;
            for (uint i = 0; i < max; i++) {
                Item empty_item;
                if (i == 0) {
                    if (pattern_variable_order[0].first == Positon::kSubject &&
                        pattern_variable_order[1].first == Positon::kPredicate) {
                        empty_item.retrieval_type = Item::RType::kGetSPreSet;
                    }
                    if (pattern_variable_order[0].first == Positon::kObject &&
                        pattern_variable_order[1].first == Positon::kPredicate) {
                        empty_item.retrieval_type = Item::RType::kGetOPreSet;
                    }
                    empty_item.empty_item_level = pattern_variable_order[1].second;
                }
                if (i == 1) {
                    if (pattern_variable_order[1].first == Positon::kPredicate &&
                        pattern_variable_order[2].first == Positon::kObject) {
                        empty_item.retrieval_type = Item::RType::kGetBySP;
                    }
                    if (pattern_variable_order[1].first == Positon::kPredicate &&
                        pattern_variable_order[2].first == Positon::kSubject) {
                        empty_item.retrieval_type = Item::RType::kGetByOP;
                    }
                    empty_item.father_item_id = pattern_variable_order[0].second;
                    empty_item.empty_item_level =
                        (distinct_predicate_) ? 0 : pattern_variable_order[2].second;
                }
                if (i == 2) {
                    empty_item.retrieval_type = Item::RType::kNone;
                    empty_item.empty_item_level = 0;
                }

                empty_item.search_id = 0;
                empty_item.prestore_type = Item::PType::kEmpty;
                empty_item.index_result = std::span<uint>();
                empty_item.triple_pattern_id = triple_pattern_id;
                uint level = pattern_variable_order[i].second;
                query_plan_[level].push_back(empty_item);
                empty_item_indices_[level].push_back(query_plan_[level].size() - 1);
            }
        }
        triple_pattern_id++;
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