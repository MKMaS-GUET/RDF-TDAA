#include "rdf-tdaa/query/query_executor.hpp"

QueryExecutor::Stat::Stat(const std::vector<std::vector<PlanGenerator::Item>>& p)
    : at_end_(false), level_(-1), plan_(p) {
    size_t n = plan_.size();
    candidate_indices_.resize(n);
    candidate_value_.resize(n);
    current_tuple_.resize(n);

    for (long unsigned int i = 0; i < n; i++) {
        candidate_value_[i] = std::make_shared<std::vector<uint>>();
    }
}

QueryExecutor::Stat::Stat(const QueryExecutor::Stat& other)
    : at_end_(other.at_end_),
      level_(other.level_),
      current_tuple_(other.current_tuple_),
      candidate_value_(other.candidate_value_),
      candidate_indices_(other.candidate_indices_),
      result_(other.result_),
      plan_(other.plan_) {}

QueryExecutor::Stat& QueryExecutor::Stat::operator=(const Stat& other) {
    if (this != &other) {
        at_end_ = other.at_end_;
        level_ = other.level_;
        candidate_indices_ = other.candidate_indices_;
        current_tuple_ = other.current_tuple_;
        candidate_value_ = other.candidate_value_;
        result_ = other.result_;
        plan_ = other.plan_;
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
      limit_(limit),
      shared_cnt_(shared_cnt) {}

std::shared_ptr<std::vector<uint>> QueryExecutor::LeapfrogJoin(
    std::vector<std::shared_ptr<std::vector<uint>>>& lists) {
    JoinList join_list;
    join_list.AddVectors(lists);

    return LeapfrogJoin(join_list);
}

std::shared_ptr<std::vector<uint>> QueryExecutor::LeapfrogJoin(JoinList& lists) {
    std::shared_ptr<std::vector<uint>> result_set = std::make_shared<std::vector<uint>>();

    if (lists.Size() == 1) {
        std::shared_ptr<std::vector<uint>> result = std::make_shared<std::vector<uint>>();
        for (uint i = 0; i < lists.GetRangeByIndex(0)->size(); i++)
            result->push_back(lists.GetRangeByIndex(0)->operator[](i));
        return result;
    }

    // Check if any index is empty => Intersection empty
    if (lists.HasEmpty())
        return result_set;

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

    return result_set;
}

bool QueryExecutor::PreJoin() {
    JoinList join_list;
    std::stringstream key;
    pre_join_ = std::vector<std::shared_ptr<std::vector<uint>>>(stat_.plan_.size());
    for (long unsigned int level = 0; level < stat_.plan_.size(); level++) {
        if (!empty_item_indices_[level].empty())
            continue;
        if (pre_results_[level].size())
            join_list.AddVectors(pre_results_[level]);

        for (long unsigned int i = 0; i < stat_.plan_[level].size(); i++) {
            if (stat_.plan_[level][i].prestore_type_ != PlanGenerator::Item::PType::kEmpty)
                join_list.AddVector(stat_.plan_[level][i].index_result_);
        }
        if (join_list.Size() > 1) {
            pre_join_[level] = LeapfrogJoin(join_list);
            if (pre_join_[level]->size() == 0)
                return false;
        }
        join_list.Clear();
    }
    return true;
}

void QueryExecutor::Down(Stat& stat) {
    ++stat.level_;

    // 如果当前层没有查询结果，就生成结果
    if (stat.candidate_value_[stat.level_]->empty()) {
        GenCondidateValue(stat);
        if (stat.at_end_)
            return;
    }

    // 遍历当前 level_ 所有经过连接的得到的结果实体
    // 并将这些实体添加到存储结果的 current_tuple_ 中
    bool success = UpdateCurrentTuple(stat);
    while (!success && !stat.at_end_) {
        success = UpdateCurrentTuple(stat);
    }
}

void QueryExecutor::Up(Stat& stat) {
    // 清除较高 level_ 的查询结果
    stat.candidate_value_[stat.level_]->clear();
    stat.candidate_indices_[stat.level_] = 0;

    --stat.level_;
}

void QueryExecutor::Next(Stat& stat) {
    // 当前 level_ 的下一个 candidate_value_
    stat.at_end_ = false;
    bool success = UpdateCurrentTuple(stat);
    while (!success && !stat.at_end_) {
        success = UpdateCurrentTuple(stat);
    }
}

void QueryExecutor::GenCondidateValue(Stat& stat) {
    JoinList join_list;

    bool has_unariate_result = pre_results_[stat.level_].size();
    bool has_empty_item_ = empty_item_indices_[stat.level_].size();
    bool has_filled_item = filled_item_indices_[stat.level_].size();

    join_list.AddVectors(pre_results_[stat.level_]);

    for (const auto& idx : empty_item_indices_[stat.level_])
        join_list.AddVector(stat.plan_[stat.level_][idx].index_result_);

    if ((!has_unariate_result && !has_empty_item_ && has_filled_item) ||
        (has_unariate_result && !has_empty_item_ && has_filled_item)) {
        if (pre_join_[stat_.level_] != nullptr) {
            stat.candidate_value_[stat.level_]->reserve(pre_join_[stat_.level_]->size());
            for (auto iter = pre_join_[stat_.level_]->begin(); iter != pre_join_[stat_.level_]->end(); iter++)
                stat.candidate_value_[stat.level_]->emplace_back(std::move(*iter));
            return;
        } else {
            for (const auto& idx : filled_item_indices_[stat.level_])
                join_list.AddVector(stat.plan_[stat.level_][idx].index_result_);
        }
        stat.candidate_value_[stat.level_] = LeapfrogJoin(join_list);
    }

    if ((!has_unariate_result && has_empty_item_ && has_filled_item) ||
        (has_unariate_result && !has_empty_item_ && !has_filled_item && join_list.Size() == 1) ||
        (!has_unariate_result && has_empty_item_ && !has_filled_item && join_list.Size() == 1)) {
        std::shared_ptr<std::vector<uint>> range = join_list.GetRangeByIndex(0);
        stat.candidate_value_[stat.level_] = std::make_shared<std::vector<uint>>();
        for (uint i = 0; i < range->size(); i++) {
            stat.candidate_value_[stat.level_]->push_back(range->operator[](i));
        }
    }

    if ((has_unariate_result && has_empty_item_ && !has_filled_item) ||
        (has_unariate_result && has_empty_item_ && has_filled_item) ||
        (has_unariate_result && !has_empty_item_ && !has_filled_item && join_list.Size() > 1) ||
        (!has_unariate_result && has_empty_item_ && !has_filled_item && join_list.Size() > 1)) {
        stat.candidate_value_[stat.level_] = LeapfrogJoin(join_list);
    }

    // 变量的交集为空
    if (stat.candidate_value_[stat.level_]->empty()) {
        stat.at_end_ = true;
        return;
    }
}

bool QueryExecutor::UpdateCurrentTuple(Stat& stat) {
    // stat.indices_ 用于更新已经处理过的交集结果在结果列表中的 id
    size_t idx = stat.candidate_indices_[stat.level_];

    if (idx < stat.candidate_value_[stat.level_]->size()) {
        // candidate_value_ 是每一个 level_ 交集的计算结果，
        // entity 是第 idx 个结果
        uint value = stat.candidate_value_[stat.level_]->at(idx);

        // 如果当前层没有 filled_item，就说明不需要填充 filled_item 对应的 empty_item
        // 否则就要调用 FillEmptyItem 进行填充
        if (filled_item_indices_[stat.level_].empty() || FillEmptyItem(stat, value)) {
            stat.current_tuple_[stat.level_] = value;
            stat.candidate_indices_[stat.level_]++;
            return true;
        }

        // 如果当前层存在 filled_item，并且当前的 value 无法填充 empty_item
        // 则说明 value 并不是查询结果中的一个，跳过
        stat.candidate_indices_[stat.level_]++;
    } else {
        stat.at_end_ = true;
    }

    return false;
}

bool QueryExecutor::FillEmptyItem(Stat& stat, uint value) {
    bool match = true;
    // 遍历一个变量（level_）的在所有三元组中的查询结果
    for (auto& item : stat.plan_[stat.level_]) {
        for (auto& empty_item : stat.plan_[item.empty_item_level_]) {
            // 确保 search_id 相同，即在一个三元组中
            if (empty_item.triple_pattern_id_ != item.triple_pattern_id_)
                continue;

            uint id = item.search_id_;
            std::shared_ptr<std::vector<uint>> r;

            if (item.retrieval_type_ == Rtype::kSO) {
                if (item.prestore_type_ == Ptype::kObject)
                    empty_item.index_result_ = index_->GetBySO(id, value);
                else if (item.prestore_type_ == Ptype::kSubject)
                    empty_item.index_result_ = index_->GetBySO(value, id);
            }

            if (item.retrieval_type_ == Rtype::kSP) {
                if (item.prestore_type_ == Ptype::kPredicate)
                    r = index_->GetBySP(id, value);
                else if (item.prestore_type_ == Ptype::kPreSub)
                    r = index_->GetBySP(value, id);
                if (r)
                    empty_item.index_result_ = r;
                else
                    empty_item.index_result_->clear();
            }

            if (item.retrieval_type_ == Rtype::kOP) {
                if (item.prestore_type_ == Ptype::kPreObj)
                    r = index_->GetByOP(value, id);
                else if (item.prestore_type_ == Ptype::kPredicate)
                    r = index_->GetByOP(id, value);
                if (r)
                    empty_item.index_result_ = r;
                else
                    empty_item.index_result_->clear();
            }

            if (empty_item.index_result_->size() == 0)
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
        if (stat_.at_end_) {
            if (stat_.level_ == 0) {
                break;
            }
            Up(stat_);
            Next(stat_);
        } else {
            // 补完一个查询结果
            if (stat_.level_ == int(stat_.plan_.size() - 1)) {
                stat_.result_.push_back(stat_.current_tuple_);
                if (stat_.result_.size() >= limit_) {
                    break;
                }
                Next(stat_);
            } else {
                Down(stat_);
            }
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    query_duration_ = end - begin;
}

double QueryExecutor::query_duration() {
    return query_duration_.count();
}

std::vector<std::vector<uint>>& QueryExecutor::result() {
    return stat_.result_;
}