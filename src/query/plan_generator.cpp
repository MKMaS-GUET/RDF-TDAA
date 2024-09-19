#include "rdf-tdaa/query/plan_generator.hpp"

using Term = SPARQLParser::Term;

PlanGenerator::Item::Item(const Item& other)
    : search_type_(other.search_type_),
      search_code_(other.search_code_),
      candidate_result_idx_(other.candidate_result_idx_),
      search_result_(other.search_result_) {}

PlanGenerator::Item& PlanGenerator::Item::operator=(const Item& other) {
    if (this != &other) {
        search_type_ = other.search_type_;
        search_code_ = other.search_code_;
        candidate_result_idx_ = other.candidate_result_idx_;
        search_result_ = other.search_result_;
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

PlanGenerator::PlanGenerator(const std::shared_ptr<IndexRetriever>& index,
                             const std::vector<SPARQLParser::TriplePattern>& triple_partterns) {
    Generate(index, triple_partterns);
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

void PlanGenerator::Generate(const std::shared_ptr<IndexRetriever>& index,
                             const std::vector<SPARQLParser::TriplePattern>& triple_partterns) {
    bool debug = false;

    AdjacencyList query_graph_ud;
    hash_map<std::string, uint> est_size;
    hash_map<std::string, Variable> univariates;

    for (const auto& triple_parttern : triple_partterns) {
        auto& s = triple_parttern.subject_;
        auto& p = triple_parttern.predicate_;
        auto& o = triple_parttern.object_;

        uint size = 0;
        std::string v_value;
        if (s.IsVariable() && !p.IsVariable() && !o.IsVariable()) {
            v_value = s.value_;
            size = index->GetByOPSize(index->Term2ID(o), index->Term2ID(p));
            if (est_size[s.value_] == 0 || est_size[s.value_] > size)
                est_size[s.value_] = size;
        }
        if (!s.IsVariable() && p.IsVariable() && !o.IsVariable()) {
            v_value = p.value_;
            size = index->GetBySOSize(index->Term2ID(s), index->Term2ID(p));
            if (est_size[p.value_] == 0 || est_size[p.value_] > size)
                est_size[p.value_] = size;
        }
        if (!s.IsVariable() && !p.IsVariable() && o.IsVariable()) {
            v_value = o.value_;
            size = index->GetBySPSize(index->Term2ID(s), index->Term2ID(p));
            if (est_size[s.value_] == 0 || est_size[s.value_] > size)
                est_size[s.value_] = size;
        }
        if (triple_parttern.variale_cnt_ == 1) {
            if (size == 0) {
                zero_result_ = true;
                return;
            }
            auto it = univariates.find(v_value);
            if (it != univariates.end()) {
                it->second.count_++;
            } else {
                univariates.insert({v_value, Variable(v_value)});
            }
        }
    }

    for (const auto& triple_parttern : triple_partterns) {
        auto& s = triple_parttern.subject_;
        auto& p = triple_parttern.predicate_;
        auto& o = triple_parttern.object_;

        std::string vertex1;
        std::string vertex2;
        uint edge = 0;
        uint size = 0;

        if (s.IsVariable() && !p.IsVariable() && o.IsVariable()) {
            vertex1 = s.value_;
            vertex2 = o.value_;
            edge = index->Term2ID(p);
            size = index->GetSSetSize(edge);
            if (est_size[s.value_] == 0 || est_size[s.value_] > size)
                est_size[s.value_] = size;
            size = index->GetOSetSize(edge);
            if (est_size[o.value_] == 0 || est_size[o.value_] > size)
                est_size[o.value_] = size;
        }
        if (s.IsVariable() && p.IsVariable() && !o.IsVariable()) {
            vertex1 = s.value_;
            vertex2 = p.value_;
            edge = index->Term2ID(o);
        }
        if (!s.IsVariable() && p.IsVariable() && o.IsVariable()) {
            vertex1 = p.value_;
            vertex2 = o.value_;
            edge = index->Term2ID(s);
        }

        if (triple_parttern.variale_cnt_ == 2) {
            query_graph_ud[vertex1].push_back({vertex2, edge});
            query_graph_ud[vertex2].push_back({vertex1, edge});
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
        std::cout << univariates.begin()->first << std::endl;
        GenPlanTable(index, triple_partterns);
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

    GenPlanTable(index, triple_partterns);
}

void PlanGenerator::GenPlanTable(const std::shared_ptr<IndexRetriever>& index,
                                 const std::vector<SPARQLParser::TriplePattern>& triple_partterns) {
    for (size_t i = 0; i < variable_order_.size(); ++i) {
        variable_order_[i].priority_ = i;
        value2variable_[variable_order_[i].value_] = &variable_order_[i];
    }

    size_t n = variable_order_.size();
    query_plan_.resize(n);
    prestores_.resize(n);
    other_type_.resize(n);
    none_type_.resize(n);

    for (const auto& triple_parttern : triple_partterns) {
        auto& s = triple_parttern.subject_;
        auto& p = triple_parttern.predicate_;
        auto& o = triple_parttern.object_;

        // handle the situation of (?s p ?o)
        uint s_var_id = 0;
        uint p_var_id = 0;
        uint o_var_id = 0;

        if (s.IsVariable() && o.IsVariable()) {
            s_var_id = value2variable_[s.value_]->priority_;
            o_var_id = value2variable_[o.value_]->priority_;

            value2variable_[s.value_]->position_ = Term::Positon::kSubject;
            value2variable_[o.value_]->position_ = Term::Positon::kObject;

            Item item, candidate_result_item;

            // id 越小优先级越高
            if (s_var_id < o_var_id) {
                // 先在 ps 索引树上根据已知的 p 查找所有的 s
                item.search_type_ = Item::TypeT::kPO;
                item.search_code_ = index->Term2ID(p);
                // 下一步应该查询的变量的索引
                item.candidate_result_idx_ = o_var_id;
                item.search_result_ = index->GetSSet(item.search_code_);

                query_plan_[s_var_id].push_back(item);
                // 非 none 的 item 的索引
                other_type_[s_var_id].push_back(query_plan_[s_var_id].size() - 1);

                // 然后根据每一对 ps 找到对应的 o
                // 先用none来占位
                candidate_result_item.search_type_ = Item::TypeT::kNone;
                candidate_result_item.search_code_ = item.search_code_;  // don't have the search code
                candidate_result_item.candidate_result_idx_ = 0;         // don't have the candidate result
                candidate_result_item.search_result_ =
                    std::make_shared<std::vector<uint>>();  // initialize search range state
                query_plan_[o_var_id].push_back(candidate_result_item);
                // none item 的索引
                none_type_[o_var_id].push_back(query_plan_[o_var_id].size() - 1);
            } else {
                item.search_type_ = Item::TypeT::kPS;
                item.search_code_ = index->Term2ID(p);
                item.candidate_result_idx_ = s_var_id;
                item.search_result_ = index->GetOSet(item.search_code_);

                query_plan_[o_var_id].push_back(item);
                other_type_[o_var_id].push_back(query_plan_[o_var_id].size() - 1);

                candidate_result_item.search_type_ = Item::TypeT::kNone;
                candidate_result_item.search_code_ = item.search_code_;  // don't have the search code
                candidate_result_item.candidate_result_idx_ = 0;         // don't have the candidate result
                candidate_result_item.search_result_ =
                    std::make_shared<std::vector<uint>>();  // initialize search range state
                query_plan_[s_var_id].push_back(candidate_result_item);
                none_type_[s_var_id].push_back(query_plan_[s_var_id].size() - 1);
            }
        }
        // handle the situation of (?s p o)
        else if (s.IsVariable()) {
            s_var_id = value2variable_[s.value_]->priority_;
            value2variable_[s.value_]->position_ = Term::Positon::kSubject;
            uint oid = index->Term2ID(o);
            uint pid = index->Term2ID(p);
            std::shared_ptr<std::vector<uint>> r = index->GetByOP(oid, pid);
            prestores_[s_var_id].push_back(r);
        }
        // handle the situation of (s p ?o)
        else if (o.IsVariable()) {
            o_var_id = value2variable_[o.value_]->priority_;
            value2variable_[o.value_]->position_ = Term::Positon::kObject;
            uint sid = index->Term2ID(s);
            uint pid = index->Term2ID(p);
            std::shared_ptr<std::vector<uint>> r = index->GetBySP(sid, pid);
            prestores_[o_var_id].push_back(r);
        }
        // handle the situation of (s ?p o)
        else if (p.IsVariable()) {
            p_var_id = value2variable_[p.value_]->priority_;
            value2variable_[p.value_]->position_ = Term::Positon::kPredicate;
            uint sid = index->Term2ID(s);
            uint pid = index->Term2ID(p);
            std::shared_ptr<std::vector<uint>> r = index->GetBySO(sid, pid);
            prestores_[p_var_id].push_back(r);
        }
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

std::vector<std::vector<size_t>>& PlanGenerator::other_type() {
    return other_type_;
}

std::vector<std::vector<size_t>>& PlanGenerator::none_type() {
    return none_type_;
}

std::vector<std::vector<std::shared_ptr<std::vector<uint>>>>& PlanGenerator::prestores() {
    return prestores_;
}

bool PlanGenerator::zero_result() {
    return zero_result_;
}