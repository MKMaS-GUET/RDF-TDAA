#include "rdf-tdaa/query/plan_generator.hpp"

using Term = SPARQLParser::Term;
using Positon = Term::Positon;
using PType = PlanGenerator::Item::PType;
using RType = PlanGenerator::Item::RType;

PlanGenerator::Item::Item(const Item& other)
    : retrieval_type_(other.retrieval_type_),
      prestore_type_(other.prestore_type_),
      index_result_(other.index_result_),
      triple_pattern_id_(other.triple_pattern_id_),
      search_id_(other.search_id_),
      empty_item_level_(other.empty_item_level_) {}

PlanGenerator::Item& PlanGenerator::Item::operator=(const Item& other) {
    if (this != &other) {
        retrieval_type_ = other.retrieval_type_;
        prestore_type_ = other.prestore_type_;
        index_result_ = other.index_result_;
        triple_pattern_id_ = other.triple_pattern_id_;
        search_id_ = other.search_id_;
        empty_item_level_ = other.empty_item_level_;
    }
    return *this;
}

PlanGenerator::Variable::Variable() : value_(""), count_(0) {}

PlanGenerator::Variable::Variable(std::string value) : value_(value), count_(1) {}

PlanGenerator::Variable& PlanGenerator::Variable::operator=(const Variable& other) {
    if (this != &other) {
        value_ = other.value_;
        priority_ = other.priority_;
        count_ = other.count_;
        position_ = other.position_;
    }
    return *this;
}

PlanGenerator::PlanGenerator(std::shared_ptr<IndexRetriever>& index,
                             std::shared_ptr<std::vector<SPARQLParser::TriplePattern>>& triple_partterns)
    : index_(index), triple_partterns_(triple_partterns) {
    Generate();
}

void PlanGenerator::DFS(const AdjacencyList& graph,
                        std::string vertex,
                        hash_map<std::string, bool>& visited,
                        AdjacencyList& tree,
                        std::vector<std::string>& currentPath,
                        std::vector<std::vector<std::string>>& allPaths) {
    currentPath.push_back(vertex);  // Add the current vertex to the path

    // Check if it's a leaf node in the spanning tree (no adjacent vertices)
    // if (graph.at(vertex).size() == 1 || visited[vertex] == true) {
    //     allPaths.push_back(currentPath);  // Save the current path if it's a leaf
    // }
    bool all_visited = true;
    for (const auto& edge : graph.at(vertex)) {
        if (!visited[edge.first]) {
            all_visited = false;
        }
    }
    if (all_visited)
        allPaths.push_back(currentPath);
    visited[vertex] = true;

    // Explore the adjacent vertices
    for (const auto& edge : graph.at(vertex)) {
        std::string adjVertex = edge.first;
        if (!visited[adjVertex]) {
            tree[vertex].emplace_back(adjVertex, edge.second);            // Add edge to the spanning tree
            DFS(graph, adjVertex, visited, tree, currentPath, allPaths);  // Continue DFS
        }
    }

    // Backtrack: remove the current vertex from the path
    currentPath.pop_back();
}

std::vector<std::vector<std::string>> PlanGenerator::FindAllPathsInGraph(const AdjacencyList& graph,
                                                                         const std::string& root) {
    hash_map<std::string, bool> visited;             // Track visited vertices
    AdjacencyList tree;                              // The resulting spanning tree
    std::vector<std::string> currentPath;            // Current path from the root to the current vertex
    std::vector<std::vector<std::string>> allPaths;  // All paths from the root to the leaves

    // Initialize visited map
    for (const auto& pair : graph) {
        visited[pair.first] = false;
    }
    // visited[root] = true;

    // Perform DFS to fill the spanning tree and find all paths
    DFS(graph, root, visited, tree, currentPath, allPaths);

    return allPaths;
}

void PlanGenerator::Generate() {
    bool debug = false;

    AdjacencyList query_graph_ud;
    hash_map<std::string, uint> est_size;
    hash_map<std::string, Variable> univariates;

    // one variable
    for (const auto& triple_parttern : *triple_partterns_) {
        auto& s = triple_parttern.subject_;
        auto& p = triple_parttern.predicate_;
        auto& o = triple_parttern.object_;

        uint size = 0;
        std::string v_value;
        if (s.IsVariable() && !p.IsVariable() && !o.IsVariable()) {
            v_value = s.value_;
            size = index_->GetByOPSize(index_->Term2ID(o), index_->Term2ID(p));
        }
        if (!s.IsVariable() && p.IsVariable() && !o.IsVariable()) {
            v_value = p.value_;
            size = index_->GetBySOSize(index_->Term2ID(s), index_->Term2ID(o));
        }
        if (!s.IsVariable() && !p.IsVariable() && o.IsVariable()) {
            v_value = o.value_;
            size = index_->GetBySPSize(index_->Term2ID(s), index_->Term2ID(p));
        }
        if (triple_parttern.variale_cnt_ == 1) {
            if (size == 0) {
                zero_result_ = true;
                return;
            }
            if (est_size[v_value] == 0 || est_size[v_value] > size)
                est_size[v_value] = size;
            auto it = univariates.find(v_value);
            if (it != univariates.end()) {
                it->second.count_++;
            } else {
                univariates.insert({v_value, Variable(v_value)});
            }
        }
    }

    // two variables
    for (const auto& triple_parttern : *triple_partterns_) {
        auto& s = triple_parttern.subject_;
        auto& p = triple_parttern.predicate_;
        auto& o = triple_parttern.object_;

        std::string vertex1;
        std::string vertex2;
        uint edge = 0;
        uint size1 = 0;
        uint size2 = 0;

        if (s.IsVariable() && !p.IsVariable() && o.IsVariable()) {
            vertex1 = s.value_;
            vertex2 = o.value_;
            edge = index_->Term2ID(p);
            size1 = index_->GetSSetSize(edge);
            size2 = index_->GetOSetSize(edge);
        }
        if (!s.IsVariable() && p.IsVariable() && o.IsVariable()) {
            vertex1 = p.value_;
            vertex2 = o.value_;
            edge = index_->Term2ID(s);
            size1 = index_->GetSPreSet(edge)->size();
            size2 = index_->GetBySSize(edge);
        }
        if (s.IsVariable() && p.IsVariable() && !o.IsVariable()) {
            vertex1 = s.value_;
            vertex2 = p.value_;
            edge = index_->Term2ID(o);
            size1 = index_->GetOPreSet(edge)->size();
            size2 = index_->GetByOSize(edge);
        }

        if (triple_parttern.variale_cnt_ == 2) {
            if (edge == 0) {
                zero_result_ = true;
                return;
            }
            query_graph_ud[vertex1].push_back({vertex2, edge});
            query_graph_ud[vertex2].push_back({vertex1, edge});
            if (est_size[vertex1] == 0 || est_size[vertex1] > size1)
                est_size[vertex1] = size1;
            if (est_size[vertex2] == 0 || est_size[vertex2] > size2)
                est_size[vertex2] = size2;
        }
    }

    if (debug) {
        for (auto vertex_it = query_graph_ud.begin(); vertex_it != query_graph_ud.end(); vertex_it++) {
            std::cout << vertex_it->first << ": ";
            for (auto& edge : vertex_it->second) {
                std::cout << " (" << edge.first << "," << edge.second << ") ";
            }
            std::cout << std::endl;
        }

        for (auto& pair : est_size) {
            std::cout << pair.first << ": " << pair.second << std::endl;
        }
    }

    std::vector<std::string> variable_priority(query_graph_ud.size());
    std::transform(query_graph_ud.begin(), query_graph_ud.end(), variable_priority.begin(),
                   [](const auto& pair) { return pair.first; });

    if (variable_priority.size() == 0 && univariates.size() == 1) {
        variable_order_.push_back(univariates.begin()->second);
        GenPlanTable();
        return;
    }

    std::sort(variable_priority.begin(), variable_priority.end(), [&](const auto& var1, const auto& var2) {
        if (query_graph_ud[var1].size() + univariates[var1].count_ !=
            query_graph_ud[var2].size() + univariates[var2].count_) {
            return query_graph_ud[var1].size() + univariates[var1].count_ >
                   query_graph_ud[var2].size() + univariates[var2].count_;
        }
        return est_size[var1] < est_size[var2];
    });

    if (debug) {
        std::cout << "------------------------------" << std::endl;
        for (auto& v : variable_priority) {
            std::cout << v << ": " << est_size[v] << std::endl;
        }
    }

    std::vector<std::string> one_degree_variables;
    uint degree_two = 0;
    uint other_degree = 0;
    for (auto vertex_it = query_graph_ud.begin(); vertex_it != query_graph_ud.end(); vertex_it++) {
        if (vertex_it->second.size() + univariates[vertex_it->first].count_ == 1) {
            one_degree_variables.push_back(vertex_it->first);
        } else if (vertex_it->second.size() + univariates[vertex_it->first].count_ == 2) {
            degree_two++;
        } else {
            other_degree++;
        }
    }

    std::vector<std::vector<std::string>> allPaths;
    std::vector<std::vector<std::string>> partialPaths;
    uint longest_path = 0;
    while (variable_priority.size() > 0) {
        partialPaths = FindAllPathsInGraph(query_graph_ud, variable_priority[0]);
        for (auto& path : partialPaths) {
            for (auto& v : path) {
                for (auto it = variable_priority.begin(); it != variable_priority.end(); it++) {
                    if (*it == v) {
                        variable_priority.erase(it);
                        break;
                    }
                }
            }
            if (allPaths.size() == 0 || path.size() > allPaths[longest_path].size()) {
                longest_path = allPaths.size();
            }
            allPaths.push_back(path);
        }
    }

    if (!(one_degree_variables.size() == query_graph_ud.size() - 1) &&  // 非star
        ((one_degree_variables.size() == 2 &&
          one_degree_variables.size() + degree_two == query_graph_ud.size() &&
          allPaths.size() == partialPaths.size()) ||  // 是连通图且是路径
         allPaths.size() == 1)) {                     // 是一个环
        if (allPaths.size() == 1) {                   // 是一个环/路径
            if (est_size[allPaths[0][0]] < est_size[allPaths[0].back()]) {
                for (auto it = allPaths[0].begin(); it != allPaths[0].end(); it++) {
                    variable_order_.push_back(*it);
                }
            } else {
                for (int i = allPaths[0].size() - 1; i > -1; i--) {
                    variable_order_.push_back(allPaths[0][i]);
                }
            }
        } else {  // 是一个路径
            if (est_size[one_degree_variables[0]] < est_size[one_degree_variables[1]])
                variable_order_.push_back(one_degree_variables[0]);
            else
                variable_order_.push_back(one_degree_variables[1]);
            std::string previews = variable_order_.back().value_;
            variable_order_.push_back(query_graph_ud[previews][0].first);
            while (variable_order_.size() != query_graph_ud.size()) {
                if (query_graph_ud[variable_order_.back().value_][0].first != previews) {
                    previews = variable_order_.back().value_;
                    variable_order_.push_back(query_graph_ud[variable_order_.back().value_][0].first);
                } else {
                    previews = variable_order_.back().value_;
                    variable_order_.push_back(query_graph_ud[variable_order_.back().value_][1].first);
                };
            }
        }
    } else {
        phmap::flat_hash_set<std::string> exist_variables;
        for (auto& v : allPaths[longest_path]) {
            exist_variables.insert(v);
        }
        for (uint i = 0; i < allPaths.size(); i++) {
            if (i != longest_path) {
                for (auto it = allPaths[i].begin(); it != allPaths[i].end(); it++) {
                    if (exist_variables.contains(*it)) {
                        allPaths[i].erase(it);
                        it--;
                    } else {
                        exist_variables.insert(*it);
                    }
                }
            }
        }

        if (debug) {
            std::cout << "longest_path: " << longest_path << std::endl;
            for (auto& path : allPaths) {
                for (auto it = path.begin(); it != path.end(); it++) {
                    std::cout << *it << " ";
                }
                std::cout << std::endl;
            }
        }

        std::vector<std::string>::iterator path_its[allPaths.size()];
        for (uint i = 0; i < allPaths.size(); i++) {
            path_its[i] = allPaths[i].begin();
        }

        for (uint i = 0; i < exist_variables.size() - allPaths.size(); i++) {
            std::string min_variable = "";
            uint path_id = 0;
            for (uint p = 0; p < allPaths.size(); p++) {
                if (path_its[p] != allPaths[p].end() - 1) {
                    if (min_variable == "" ||
                        query_graph_ud[*path_its[p]].size() + univariates[*path_its[p]].count_ >
                            query_graph_ud[min_variable].size() + univariates[min_variable].count_) {
                        min_variable = *path_its[p];
                        path_id = p;
                    }
                }
            }
            if (path_its[path_id] != allPaths[path_id].end() - 1)
                path_its[path_id]++;
            variable_order_.push_back(min_variable);
        }

        for (auto& path : allPaths) {
            variable_order_.push_back(*(path.end() - 1));
        }
    }

    for (auto it = univariates.begin(); it != univariates.end(); it++) {
        bool contains = false;
        for (auto& v : variable_order_) {
            if (v.value_ == it->first) {
                contains = true;
            }
        }
        if (!contains) {
            variable_order_.push_back(it->first);
        }
    }

    if (debug) {
        std::cout << "variables order: " << std::endl;
        for (auto it = variable_order_.begin(); it != variable_order_.end(); it++) {
            std::cout << it->value_ << " ";
        }
        std::cout << std::endl;
        std::cout << "------------------------------" << std::endl;
    }

    GenPlanTable();
}

void PlanGenerator::GenPlanTable() {
    for (size_t i = 0; i < variable_order_.size(); ++i) {
        variable_order_[i].priority_ = i;
        value2variable_[variable_order_[i].value_] = &variable_order_[i];
    }

    size_t n = variable_order_.size();
    query_plan_.resize(n);
    univariate_results_.resize(n);
    filled_item_indices_.resize(n);
    empty_item_indices_.resize(n);

    uint triple_pattern_id = 0;
    for (const auto& triple_parttern : *triple_partterns_) {
        auto& s = triple_parttern.subject_;
        auto& p = triple_parttern.predicate_;
        auto& o = triple_parttern.object_;

        uint s_var_id = 0;
        uint p_var_id = 0;
        uint o_var_id = 0;

        if (triple_parttern.variale_cnt_ == 2) {
            Item filled_item, empty_item;
            uint first_id, second_id;
            bool is_first_prior = false;

            auto process_filled_item = [&](const Term& fixed_term, const Term& var_term1,
                                           const Term& var_term2, Positon var1_position,
                                           Positon var2_position, PType prestore_type1, PType prestore_type2,
                                           RType retrieval_type1, RType retrieval_type2, auto index_func1,
                                           auto index_func2) {
                value2variable_[var_term1.value_]->position_ = var1_position;
                value2variable_[var_term2.value_]->position_ = var2_position;

                first_id = value2variable_[var_term1.value_]->priority_;
                second_id = value2variable_[var_term2.value_]->priority_;

                is_first_prior = first_id < second_id;

                filled_item.search_id_ = index_->Term2ID(fixed_term);
                filled_item.prestore_type_ = is_first_prior ? prestore_type1 : prestore_type2;
                filled_item.retrieval_type_ = is_first_prior ? retrieval_type1 : retrieval_type2;
                filled_item.index_result_ = is_first_prior ? ((*index_).*index_func1)(filled_item.search_id_)
                                                           : ((*index_).*index_func2)(filled_item.search_id_);
            };

            if (!s.IsVariable() && p.IsVariable() && o.IsVariable()) {
                process_filled_item(s, p, o, Positon::kPredicate, Positon::kObject, PType::kPredicate,
                                    PType::kObject, RType::kSP, RType::kSO, &IndexRetriever::GetSPreSet,
                                    &IndexRetriever::GetByS);
            } else if (s.IsVariable() && !p.IsVariable() && o.IsVariable()) {
                process_filled_item(p, s, o, Positon::kSubject, Positon::kObject, PType::kPreSub,
                                    PType::kPreObj, RType::kSP, RType::kOP, &IndexRetriever::GetSSet,
                                    &IndexRetriever::GetOSet);
            } else if (s.IsVariable() && p.IsVariable() && !o.IsVariable()) {
                process_filled_item(o, s, p, Positon::kSubject, Positon::kPredicate, PType::kSubject,
                                    PType::kPredicate, RType::kSO, RType::kOP, &IndexRetriever::GetByO,
                                    &IndexRetriever::GetOPreSet);
            }

            uint higher_priority_id = is_first_prior ? first_id : second_id;
            uint lower_priority_id = is_first_prior ? second_id : first_id;

            filled_item.triple_pattern_id_ = triple_pattern_id;
            filled_item.empty_item_level_ = lower_priority_id;

            empty_item.search_id_ = filled_item.search_id_;
            empty_item.prestore_type_ = Item::PType::kEmpty;
            empty_item.retrieval_type_ = Item::RType::kNone;
            empty_item.index_result_ = std::make_shared<std::vector<uint>>();
            empty_item.triple_pattern_id_ = triple_pattern_id;
            empty_item.empty_item_level_ = 0;

            query_plan_[higher_priority_id].push_back(filled_item);
            filled_item_indices_[higher_priority_id].push_back(query_plan_[higher_priority_id].size() - 1);
            query_plan_[lower_priority_id].push_back(empty_item);
            empty_item_indices_[lower_priority_id].push_back(query_plan_[lower_priority_id].size() - 1);
        }

        if (s.IsVariable() && !p.IsVariable() && !o.IsVariable()) {
            s_var_id = value2variable_[s.value_]->priority_;
            value2variable_[s.value_]->position_ = Term::Positon::kSubject;
            uint oid = index_->Term2ID(o);
            uint pid = index_->Term2ID(p);
            std::shared_ptr<std::vector<uint>> r = index_->GetByOP(oid, pid);
            univariate_results_[s_var_id].push_back(r);
        }
        if (!s.IsVariable() && p.IsVariable() && !o.IsVariable()) {
            p_var_id = value2variable_[p.value_]->priority_;
            value2variable_[p.value_]->position_ = Term::Positon::kPredicate;
            uint sid = index_->Term2ID(s);
            uint oid = index_->Term2ID(o);
            std::shared_ptr<std::vector<uint>> r = index_->GetBySO(sid, oid);
            univariate_results_[p_var_id].push_back(r);
        }
        if (!s.IsVariable() && !p.IsVariable() && o.IsVariable()) {
            o_var_id = value2variable_[o.value_]->priority_;
            value2variable_[o.value_]->position_ = Term::Positon::kObject;
            uint sid = index_->Term2ID(s);
            uint pid = index_->Term2ID(p);
            std::shared_ptr<std::vector<uint>> r = index_->GetBySP(sid, pid);
            univariate_results_[o_var_id].push_back(r);
        }
        triple_pattern_id++;
    }
}

std::vector<PlanGenerator::Variable> PlanGenerator::MappingVariable(
    const std::vector<std::string>& variables) {
    std::vector<Variable> ret;
    ret.reserve(variables.size());
    for (const auto& var : variables) {
        ret.push_back(*value2variable_.at(var));
    }
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

std::vector<std::vector<std::shared_ptr<std::vector<uint>>>>& PlanGenerator::univariate_results() {
    return univariate_results_;
}

bool PlanGenerator::zero_result() {
    return zero_result_;
}