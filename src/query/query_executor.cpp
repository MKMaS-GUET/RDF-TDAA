#include "rdf-tdaa/query/query_executor.hpp"

QueryExecutor::Stat::Stat(const std::vector<std::vector<PlanGenerator::Item>>& p)
    : at_end(false), level(-1), plan(p) {
    size_t n = plan.size();
    candidate_indices.resize(n);
    candidate_value.resize(n);
    current_tuple.resize(n);
    result = std::make_shared<std::vector<std::vector<uint>>>();

    for (long unsigned int i = 0; i < n; i++) {
        candidate_value[i] = std::span<uint>();
    }
}

QueryExecutor::Stat::Stat(const QueryExecutor::Stat& other)
    : at_end(other.at_end),
      level(other.level),
      current_tuple(other.current_tuple),
      candidate_value(other.candidate_value),
      candidate_indices(other.candidate_indices),
      result(other.result),
      plan(other.plan) {}

QueryExecutor::Stat& QueryExecutor::Stat::operator=(const Stat& other) {
    if (this != &other) {
        at_end = other.at_end;
        level = other.level;
        candidate_indices = other.candidate_indices;
        current_tuple = other.current_tuple;
        candidate_value = other.candidate_value;
        result = other.result;
        plan = other.plan;
    }
    return *this;
}

QueryExecutor::QueryExecutor(std::shared_ptr<IndexRetriever> index,
                             std::shared_ptr<PlanGenerator>& plan,
                             uint limit,
                             uint shared_cnt)
    : stat_(plan->query_plan()),
      index_(index),
      filled_item_indices_(plan->filled_item_indices()),
      empty_item_indices_(plan->empty_item_indices()),
      pre_results_(plan->pre_results()),
      three_variable_pattern_(plan->three_variable_pattern()),
      limit_(limit),
      shared_cnt_(shared_cnt) {}

std::span<uint> QueryExecutor::LeapfrogJoin(std::vector<std::span<uint>>& lists) {
    JoinList join_list;
    join_list.AddVectors(lists);

    return LeapfrogJoin(join_list);
}

std::span<uint> QueryExecutor::LeapfrogJoin(JoinList& lists) {
    std::vector<uint>* result_set = new std::vector<uint>();

    if (lists.Size() == 1) {
        std::vector<uint>* result = new std::vector<uint>;
        for (uint i = 0; i < lists.GetRangeByIndex(0).size(); i++)
            result->push_back(lists.GetRangeByIndex(0)[i]);
        return std::span<uint>(result->begin(), result->size());
    }

    // Check if any index is empty => Intersection empty
    if (lists.HasEmpty())
        return std::span<uint>();

    lists.UpdateCurrentPostion();
    // 创建指向每一个列表的指针，初始指向列表的第一个值

    //  max 是所有指针指向位置的最大值，初始的最大值就是对列表排序后，最后一个列表的第一个值
    size_t max = lists.GetCurrentValOfRange(lists.Size() - 1);
    // 当前迭代器的 id
    size_t idx = 0;

    uint value;
    while (true) {
        // 当前迭代器的第一个值
        value = lists.GetCurrentValOfRange(idx);

        // get_current_val_time += diff.count();
        // An intersecting value has been found!
        // 在没有找到交集中的值时，
        // 当前迭代器指向的值 (max) 都要 > 此迭代器之前的迭代器指向的值，
        // 第一个迭代器指向的值 > 最后一个迭代器指向的值，
        // 所以 max 一定大于下一个迭代器指向的值。
        // 若在迭代器 i 中的新 max 等于上一个 max 的情况，之后遍历了一遍迭代器列表再次回到迭代器 i，
        // 但是 max 依旧没有变化，此时才会出现当前迭代器的 value 与 max 相同。
        // 因为此时已经遍历了所有迭代器，都找到了相同的值，所以就找到了交集中的值
        if (value == max) {
            result_set->push_back(value);
            lists.NextVal(idx);
            // We shall find a value greater or equal than the current max
        } else {
            // 将当前迭代器指向的位置变为第一个大于 max 的值的位置
            lists.Seek(idx, max);
        }

        if (lists.AtEnd(idx)) {
            break;
        }

        // Store the maximum
        max = lists.GetCurrentValOfRange(idx);

        idx++;
        idx = idx % lists.Size();
    }

    return std::span<uint>(result_set->begin(), result_set->size());
}

bool QueryExecutor::PreJoin() {
    JoinList join_list;
    std::stringstream key;
    pre_join_ = std::vector<std::span<uint>>(stat_.plan.size());
    for (long unsigned int level = 0; level < stat_.plan.size(); level++) {
        if (!empty_item_indices_[level].empty())
            continue;
        if (pre_results_[level].size())
            join_list.AddVectors(pre_results_[level]);

        for (long unsigned int i = 0; i < stat_.plan[level].size(); i++) {
            if (stat_.plan[level][i].prestore_type != PlanGenerator::Item::PType::kEmpty)
                join_list.AddVector(stat_.plan[level][i].index_result);
        }
        if (join_list.Size() > 1) {
            pre_join_[level] = LeapfrogJoin(join_list);
            if (pre_join_[level].size() == 0)
                return false;
        }
        join_list.Clear();
    }
    return true;
}

void QueryExecutor::Down(Stat& stat) {
    ++stat.level;

    // 如果当前层没有查询结果，就生成结果
    if (stat.candidate_value[stat.level].empty()) {
        GenCondidateValue(stat);
        if (stat.at_end)
            return;
    }

    // 遍历当前 level_ 所有经过连接的得到的结果实体
    // 并将这些实体添加到存储结果的 current_tuple_ 中
    bool success = UpdateCurrentTuple(stat);
    while (!success && !stat.at_end) {
        success = UpdateCurrentTuple(stat);
    }
}

void QueryExecutor::Up(Stat& stat) {
    // 清除较高 level_ 的查询结果
    stat.candidate_value[stat.level] = std::span<uint>();
    stat.candidate_indices[stat.level] = 0;

    --stat.level;
}

void QueryExecutor::Next(Stat& stat) {
    // 当前 level_ 的下一个 candidate_value_
    stat.at_end = false;
    bool success = UpdateCurrentTuple(stat);
    while (!success && !stat.at_end) {
        success = UpdateCurrentTuple(stat);
    }
}

void QueryExecutor::GenCondidateValue(Stat& stat) {
    JoinList join_list;

    bool has_unariate_result = pre_results_[stat.level].size();
    bool has_empty_item_ = empty_item_indices_[stat.level].size();
    bool has_filled_item = filled_item_indices_[stat.level].size();

    join_list.AddVectors(pre_results_[stat.level]);

    for (const auto& idx : empty_item_indices_[stat.level])
        join_list.AddVector(stat.plan[stat.level][idx].index_result);

    if ((!has_unariate_result && !has_empty_item_ && has_filled_item) ||
        (has_unariate_result && !has_empty_item_ && has_filled_item)) {
        if (pre_join_[stat_.level].size()) {
            stat.candidate_value[stat.level] = pre_join_[stat_.level];
            return;
        } else {
            for (const auto& idx : filled_item_indices_[stat.level])
                join_list.AddVector(stat.plan[stat.level][idx].index_result);
        }
        stat.candidate_value[stat.level] = LeapfrogJoin(join_list);
    }

    if ((!has_unariate_result && has_empty_item_ && has_filled_item) ||
        (has_unariate_result && !has_empty_item_ && !has_filled_item && join_list.Size() == 1) ||
        (!has_unariate_result && has_empty_item_ && !has_filled_item && join_list.Size() == 1)) {
        stat.candidate_value[stat.level] = join_list.GetRangeByIndex(0);
    }

    if ((has_unariate_result && has_empty_item_ && !has_filled_item) ||
        (has_unariate_result && has_empty_item_ && has_filled_item) ||
        (has_unariate_result && !has_empty_item_ && !has_filled_item && join_list.Size() > 1) ||
        (!has_unariate_result && has_empty_item_ && !has_filled_item && join_list.Size() > 1)) {
        stat.candidate_value[stat.level] = LeapfrogJoin(join_list);
    }

    // 变量的交集为空
    if (stat.candidate_value[stat.level].empty()) {
        stat.at_end = true;
        return;
    }
}

bool QueryExecutor::UpdateCurrentTuple(Stat& stat) {
    // stat.indices_ 用于更新已经处理过的交集结果在结果列表中的 id
    size_t idx = stat.candidate_indices[stat.level];

    if (idx < stat.candidate_value[stat.level].size()) {
        // candidate_value_ 是每一个 level_ 交集的计算结果，
        // entity 是第 idx 个结果
        uint value = stat.candidate_value[stat.level][idx];

        // 如果当前层没有 filled_item，就说明不需要填充 filled_item 对应的 empty_item
        // 否则就要调用 FillEmptyItem 进行填充
        if (filled_item_indices_[stat.level].empty() || FillEmptyItem(stat, value)) {
            stat.current_tuple[stat.level] = value;
            stat.candidate_indices[stat.level]++;
            return true;
        }

        // 如果当前层存在 filled_item，并且当前的 value 无法填充 empty_item
        // 则说明 value 并不是查询结果中的一个，跳过
        stat.candidate_indices[stat.level]++;
    } else {
        stat.at_end = true;
    }

    return false;
}

bool QueryExecutor::FillEmptyItem(Stat& stat, uint value) {
    bool match = true;
    // 遍历一个变量（level_）的在所有三元组中的查询结果
    for (auto& item : stat.plan[stat.level]) {
        for (auto& empty_item : stat.plan[item.empty_item_level]) {
            // 确保 search_id 相同，即在一个三元组中
            if (empty_item.triple_pattern_id != item.triple_pattern_id)
                continue;

            uint id = item.search_id;
            std::span<uint> r;

            if (item.retrieval_type == Rtype::kSO) {
                if (item.prestore_type == Ptype::kObject)
                    empty_item.index_result = index_->GetBySO(id, value);
                else if (item.prestore_type == Ptype::kSubject)
                    empty_item.index_result = index_->GetBySO(value, id);
            }

            if (item.retrieval_type == Rtype::kSP) {
                if (item.prestore_type == Ptype::kPredicate)
                    r = index_->GetBySP(id, value);
                else if (item.prestore_type == Ptype::kPreSub)
                    r = index_->GetBySP(value, id);
                empty_item.index_result = r;
            }

            if (item.retrieval_type == Rtype::kOP) {
                if (item.prestore_type == Ptype::kPreObj)
                    r = index_->GetByOP(value, id);
                else if (item.prestore_type == Ptype::kPredicate)
                    r = index_->GetByOP(id, value);
                empty_item.index_result = r;
            }

            if (empty_item.index_result.size() == 0)
                match = false;
            break;
        }
    }
    return match;
}

void QueryExecutor::Query() {
    auto begin = std::chrono::high_resolution_clock::now();

    if (!PreJoin())
        return;

    for (;;) {
        if (stat_.at_end) {
            if (stat_.level == 0) {
                break;
            }
            Up(stat_);
            Next(stat_);
        } else {
            // 补完一个查询结果
            if (stat_.level == int(stat_.plan.size() - 1)) {
                stat_.result->push_back(stat_.current_tuple);
                if (stat_.result->size() >= limit_)
                    break;
                Next(stat_);
            } else {
                Down(stat_);
            }
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    query_duration_ = end - begin;
}

void QueryExecutor::HandleThreeVariablePattern(SPARQLParser::ProjectModifier modifier,
                                               const std::vector<std::string>& project_variables) {
    std::shared_ptr<std::vector<std::vector<uint>>> new_result =
        std::make_shared<std::vector<std::vector<uint>>>();

    new_result->reserve(stat_.result->size() * 2);
    phmap::flat_hash_set<uint> distinct_result;
    uint distinct_position = 0;

    uint result_tuple_size = stat_.result->at(0).size();
    uint new_result_tuple_size = result_tuple_size;

    if (three_variable_pattern_.retrieval_type == PlanGenerator::ThreeVariablePattern::kS) {
        new_result_tuple_size += 2;
        for (const auto& result : *stat_.result) {
            uint s_id = result[three_variable_pattern_.constant_variable[0]->priority];
            std::span<uint> c_set = index_->GetSPreSet(s_id);
            if (three_variable_pattern_.distinct_position != 1) {
                for (auto p_id : c_set) {
                    auto o_ids = index_->GetBySP(s_id, p_id);

                    std::vector<uint> temp_result(result);
                    temp_result.resize(result.size() + 2);

                    for (auto o_id : o_ids) {
                        temp_result[result.size()] = p_id;
                        temp_result[result.size() + 1] = o_id;
                        new_result->push_back(temp_result);
                    }
                }
            } else {
                distinct_position = result_tuple_size;
                for (auto p_id : c_set) {
                    distinct_result.insert(p_id);
                }
            }
        }
    }

    if (three_variable_pattern_.retrieval_type == PlanGenerator::ThreeVariablePattern::kO) {
        new_result_tuple_size += 2;
        for (const auto& result : *stat_.result) {
            uint o_id = result[three_variable_pattern_.constant_variable[0]->priority];
            std::span<uint> c_set = index_->GetSPreSet(o_id);
            if (three_variable_pattern_.distinct_position != 1) {
                for (auto p_id : c_set) {
                    auto s_ids = index_->GetBySP(o_id, p_id);

                    std::vector<uint> temp_result(result);
                    temp_result.resize(result.size() + 2);

                    for (auto s_id : s_ids) {
                        temp_result[result.size()] = p_id;
                        temp_result[result.size() + 1] = s_id;
                        new_result->push_back(temp_result);
                    }
                }
            } else {
                distinct_position = result_tuple_size + 1;
                for (auto p_id : c_set) {
                    distinct_result.insert(p_id);
                }
            }
        }
    }

    if (three_variable_pattern_.retrieval_type == PlanGenerator::ThreeVariablePattern::kSO) {
        new_result_tuple_size += 1;
        for (const auto& result : *stat_.result) {
            uint s_id = result[three_variable_pattern_.constant_variable[0]->priority];
            uint o_id = result[three_variable_pattern_.constant_variable[1]->priority];
            std::span<uint> p_set = index_->GetBySO(s_id, o_id);
            if (three_variable_pattern_.distinct_position != 1) {
                for (auto p_id : p_set) {
                    std::vector<uint> temp_result(result);
                    temp_result.resize(new_result_tuple_size);
                    temp_result[result_tuple_size] = p_id;
                    new_result->push_back(temp_result);
                }
            } else {
                distinct_position = result_tuple_size;
                for (auto p_id : p_set) {
                    distinct_result.insert(p_id);
                }
            }
        }
    }

    if (three_variable_pattern_.distinct_position == 1) {
        for (auto it = distinct_result.begin(); it != distinct_result.end(); it++) {
            std::vector<uint> temp_result(new_result_tuple_size);
            temp_result[distinct_position] = *it;
            new_result->push_back(temp_result);
        }
    }
    stat_.result = new_result;
}

double QueryExecutor::query_duration() {
    return query_duration_.count();
}

std::vector<std::vector<uint>>& QueryExecutor::result() {
    return *stat_.result;
}