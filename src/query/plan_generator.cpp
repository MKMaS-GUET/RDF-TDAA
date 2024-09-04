#include "rdf-tdaa/query/plan_generator.hpp"

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

PlanGenerator::PlanGenerator(const std::shared_ptr<IndexRetriever>& index,
                             const std::vector<std::vector<std::string>>& triple_list) {
    Generate(index, triple_list);
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
                             const std::vector<std::vector<std::string>>& triple_list) {
    AdjacencyList query_graph_ud;
    hash_map<std::string, uint> est_size;
    hash_map<std::string, uint> univariates;

    for (size_t i = 0; i < triple_list.size(); ++i) {
        const auto& triple = triple_list[i];
        const std::string s = triple[0];
        const std::string p = triple[1];
        const std::string o = triple[2];

        std::string vertex1;
        uint edge;
        std::string vertex2;
        uint size;

        if (s[0] == '?' && o[0] == '?') {
            vertex1 = s;
            edge = index->String2ID(p, Pos::kPredicate);
            vertex2 = o;
            // only support ?v predicate ?v
            size = index->GetSSetSize(edge);
            if (est_size[s] == 0 || est_size[s] > size) {
                est_size[s] = size;
            }
            size = index->GetOSetSize(edge);
            if (est_size[o] == 0 || est_size[o] > size) {
                est_size[o] = size;
            }
        } else if (s[0] == '?' && p[0] == '?') {
            vertex1 = s;
            edge = index->String2ID(o, Pos::kObject);
            vertex2 = p;
        } else if (p[0] == '?' && o[0] == '?') {
            vertex1 = p;
            edge = index->String2ID(s, Pos::kSubject);
            vertex2 = o;
        } else {
            if (s[0] == '?') {
                univariates[s] += 1;
                size = index->GetByOPSize(index->String2ID(o, Pos::kObject),
                                          index->String2ID(p, Pos::kPredicate));
                if (est_size[s] == 0 || est_size[s] > size) {
                    est_size[s] = size;
                }
            }
            if (o[0] == '?') {
                univariates[o] += 1;
                size = index->GetBySPSize(index->String2ID(s, Pos::kSubject),
                                          index->String2ID(p, Pos::kPredicate));
                if (est_size[s] == 0 || est_size[s] > size) {
                    est_size[s] = size;
                }
            }
            if (p[0] == '?')
                univariates[p] += 1;
            continue;
        }

        query_graph_ud[vertex1].push_back({vertex2, edge});
        query_graph_ud[vertex2].push_back({vertex1, edge});
    }

    if (debug_) {
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

    std::sort(variable_priority.begin(), variable_priority.end(), [&](const auto& var1, const auto& var2) {
        if (query_graph_ud[var1].size() + univariates[var1] !=
            query_graph_ud[var2].size() + univariates[var2]) {
            return query_graph_ud[var1].size() + univariates[var1] >
                   query_graph_ud[var2].size() + univariates[var2];
        }
        return est_size[var1] < est_size[var2];
    });

    if (debug_) {
        std::cout << "------------------------------" << std::endl;
        for (auto& v : variable_priority) {
            std::cout << v << ": " << est_size[v] << std::endl;
        }
    }

    std::vector<std::string> one_degree_variables;
    uint degree_two = 0;
    uint other_degree = 0;
    for (auto vertex_it = query_graph_ud.begin(); vertex_it != query_graph_ud.end(); vertex_it++) {
        if (vertex_it->second.size() + univariates[vertex_it->first] == 1) {
            one_degree_variables.push_back(vertex_it->first);
        } else if (vertex_it->second.size() + univariates[vertex_it->first] == 2) {
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

    std::vector<std::string> plan;

    if (!(one_degree_variables.size() == query_graph_ud.size() - 1) &&  // 非star
        ((one_degree_variables.size() == 2 &&
          one_degree_variables.size() + degree_two == query_graph_ud.size() &&
          allPaths.size() == partialPaths.size()) ||  // 是连通图且是路径
         allPaths.size() == 1)) {                     // 是一个环
        if (allPaths.size() == 1) {                   // 是一个环/路径
            if (est_size[allPaths[0][0]] < est_size[allPaths[0].back()]) {
                for (auto it = allPaths[0].begin(); it != allPaths[0].end(); it++) {
                    plan.push_back(*it);
                }
            } else {
                for (int i = allPaths[0].size() - 1; i > -1; i--) {
                    plan.push_back(allPaths[0][i]);
                }
            }
        } else {  // 是一个路径
            if (est_size[one_degree_variables[0]] < est_size[one_degree_variables[1]])
                plan.push_back(one_degree_variables[0]);
            else
                plan.push_back(one_degree_variables[1]);
            std::string previews = plan.back();
            plan.push_back(query_graph_ud[previews][0].first);
            while (plan.size() != query_graph_ud.size()) {
                if (query_graph_ud[plan.back()][0].first != previews) {
                    previews = plan.back();
                    plan.push_back(query_graph_ud[plan.back()][0].first);
                } else {
                    previews = plan.back();
                    plan.push_back(query_graph_ud[plan.back()][1].first);
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

        if (debug_) {
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
                        query_graph_ud[*path_its[p]].size() + univariates[*path_its[p]] >
                            query_graph_ud[min_variable].size() + univariates[min_variable]) {
                        min_variable = *path_its[p];
                        path_id = p;
                    }
                }
            }
            if (path_its[path_id] != allPaths[path_id].end() - 1)
                path_its[path_id]++;
            plan.push_back(min_variable);
        }

        for (auto& path : allPaths) {
            plan.push_back(*(path.end() - 1));
        }
    }

    for (auto it = univariates.begin(); it != univariates.end(); it++) {
        bool contains = false;
        for (auto& v : plan) {
            if (v == it->first) {
                contains = true;
            }
        }
        if (!contains) {
            plan.push_back(it->first);
        }
    }

    if (debug_) {
        std::cout << "variables order: " << std::endl;
        for (auto it = plan.begin(); it != plan.end(); it++) {
            std::cout << *it << " ";
        }
        std::cout << std::endl;
    }

    GenPlanTable(index, triple_list, plan);
}

void PlanGenerator::GenPlanTable(const std::shared_ptr<IndexRetriever>& index,
                                 const std::vector<std::vector<std::string>>& triple_list,
                                 std::vector<std::string>& variables) {
    for (size_t i = 0; i < variables.size(); ++i) {
        variable_metadata_[variables[i]].first = i;
        if (debug_)
            std::cout << variables[i] << ": " << i << std::endl;
    }

    size_t n = variables.size();
    query_plan_.resize(n);
    prestores_.resize(n);
    other_type_.resize(n);
    none_type_.resize(n);

    int range_cnt = 0;

    for (const auto& triple : triple_list) {
        const std::string& s = triple[0];
        const std::string& p = triple[1];
        const std::string& o = triple[2];

        if (p[0] == '?') {
            // skip
            // TODO: handle the situation of (s ?p o), (s ?p ?o), (?s ?p o), (?s ?p ?o)
            continue;
        }

        // handle the situation of (?s p ?o)
        uint64_t var_sid = variable_metadata_[s].first;
        uint64_t var_oid = variable_metadata_[o].first;
        if (s[0] == '?' && o[0] == '?') {
            variable_metadata_[s].second = Pos::kSubject;
            variable_metadata_[o].second = Pos::kObject;

            Item item, candidate_result_item;

            // id 越小优先级越高
            if (var_sid < var_oid) {
                // 先在 ps 索引树上根据已知的 p 查找所有的 s
                item.search_type_ = Item::TypeT::kPO;
                item.search_code_ = index->String2ID(p, Pos::kPredicate);
                // 下一步应该查询的变量的索引
                item.candidate_result_idx_ = var_oid;
                item.search_result_ = index->GetSSet(item.search_code_);
                item.search_result_->id = range_cnt;
                range_cnt += 1;
                query_plan_[var_sid].push_back(item);
                // 非 none 的 item 的索引
                other_type_[var_sid].push_back(query_plan_[var_sid].size() - 1);

                // 然后根据每一对 ps 找到对应的 o
                // 先用none来占位
                candidate_result_item.search_type_ = Item::TypeT::kNone;
                candidate_result_item.search_code_ = item.search_code_;  // don't have the search code
                candidate_result_item.candidate_result_idx_ = 0;         // don't have the candidate result
                candidate_result_item.search_result_ =
                    std::make_shared<Result>();  // initialize search range state
                query_plan_[var_oid].push_back(candidate_result_item);
                // none item 的索引
                none_type_[var_oid].push_back(query_plan_[var_oid].size() - 1);
            } else {
                item.search_code_ = index->String2ID(p, Pos::kPredicate);
                item.search_type_ = Item::TypeT::kPS;
                item.candidate_result_idx_ = var_sid;
                item.search_result_ = index->GetOSet(item.search_code_);
                item.search_result_->id = range_cnt;
                range_cnt += 1;
                query_plan_[var_oid].push_back(item);
                other_type_[var_oid].push_back(query_plan_[var_oid].size() - 1);

                candidate_result_item.search_type_ = Item::TypeT::kNone;
                candidate_result_item.search_code_ = item.search_code_;  // don't have the search code
                candidate_result_item.candidate_result_idx_ = 0;         // don't have the candidate result
                candidate_result_item.search_result_ =
                    std::make_shared<Result>();  // initialize search range state
                query_plan_[var_sid].push_back(candidate_result_item);
                none_type_[var_sid].push_back(query_plan_[var_sid].size() - 1);
            }
        }
        // handle the situation of (?s p o)
        else if (s[0] == '?') {
            variable_metadata_[s].second = Pos::kSubject;
            uint oid = index->String2ID(o, Pos::kObject);
            uint pid = index->String2ID(p, Pos::kPredicate);
            std::shared_ptr<Result> r = index->GetByOP(oid, pid);
            r->id = range_cnt;
            prestores_[var_sid].push_back(r);
            range_cnt++;
        }
        // handle the situation of (s p ?o)
        else if (o[0] == '?') {
            variable_metadata_[o].second = Pos::kObject;
            uint sid = index->String2ID(s, Pos::kSubject);
            uint pid = index->String2ID(p, Pos::kPredicate);
            std::shared_ptr<Result> r = index->GetBySP(sid, pid);
            r->id = range_cnt;
            prestores_[var_oid].push_back(r);
            range_cnt++;
        }
    }

    if (debug_) {
        std::cout << "prestore_result_:" << std::endl;
        for (int level = 0; level < int(prestores_.size()); level++) {
            // _prestore_result_[level].size();
            std::cout << "level: " << level << " count: " << prestores_[level].size() << std::endl;

            for (int i = 0; i < int(prestores_[level].size()); i++) {
                std::cout << "[";
                std::cout << prestores_[level][i]->Size();
                std::cout << "] ";
            }

            std::cout << std::endl;
        }
        std::cout << "---------------------------------" << std::endl;

        std::cout << "all plan: " << std::endl;
        for (int i = 0; i < int(query_plan_.size()); i++) {
            for (int j = 0; j < int(query_plan_[i].size()); j++) {
                Item item = query_plan_[i][j];
                std::cout << item.search_result_->id << " [";
                std::cout << item.search_result_->Size();
                std::cout << "] ";
            }
            std::cout << std::endl;
        }
        std::cout << "---------------------------------" << std::endl;

        std::cout << "all none: " << std::endl;
        for (int i = 0; i < int(none_type_.size()); i++) {
            std::cout << "level: " << i << " size: " << none_type_[i].size() << std::endl;
        }
        std::cout << "---------------------------------" << std::endl;
    }
}

std::vector<std::pair<uint, Pos>> PlanGenerator::MappingVariable(const std::vector<std::string>& variables) {
    std::vector<std::pair<uint, Pos>> ret;
    ret.reserve(variables.size());
    for (const auto& var : variables) {
        ret.emplace_back(variable_metadata_.at(var));
    }
    return ret;
}

std::vector<std::vector<PlanGenerator::Item>>& PlanGenerator::query_plan() {
    return query_plan_;
}

hash_map<std::string, std::pair<uint, Pos>>& PlanGenerator::variable_metadata() {
    return variable_metadata_;
}

std::vector<std::vector<size_t>>& PlanGenerator::other_type() {
    return other_type_;
}

std::vector<std::vector<size_t>>& PlanGenerator::none_type() {
    return none_type_;
}

std::vector<std::vector<std::shared_ptr<Result>>>& PlanGenerator::prestores() {
    return prestores_;
}