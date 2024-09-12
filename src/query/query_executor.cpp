#include "rdf-tdaa/query/query_executor.hpp"

QueryExecutor::Stat::Stat(const std::vector<std::vector<PlanGenerator::Item>>& p)
    : at_end_(false), level_(-1), plan_(p) {
    size_t n = plan_.size();
    indices_.resize(n);
    candidate_result_.resize(n);
    current_tuple_.resize(n);

    for (long unsigned int i = 0; i < n; i++) {
        candidate_result_[i] = std::make_shared<std::vector<uint>>();
    }
}

QueryExecutor::Stat::Stat(const QueryExecutor::Stat& other)
    : at_end_(other.at_end_),
      level_(other.level_),
      indices_(other.indices_),
      current_tuple_(other.current_tuple_),
      candidate_result_(other.candidate_result_),
      result_(other.result_),
      plan_(other.plan_) {}

QueryExecutor::Stat& QueryExecutor::Stat::operator=(const Stat& other) {
    if (this != &other) {
        at_end_ = other.at_end_;
        level_ = other.level_;
        indices_ = other.indices_;
        current_tuple_ = other.current_tuple_;
        candidate_result_ = other.candidate_result_;
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
      other_type_(plan->other_type()),
      none_type_(plan->none_type()),
      prestores_(plan->prestores()),
      limit_(limit),
      shared_cnt_(shared_cnt) {}

std::shared_ptr<std::vector<uint>> QueryExecutor::LeapfrogJoin(ResultList& indexes) {
    std::shared_ptr<std::vector<uint>> result_set = std::make_shared<std::vector<uint>>();

    if (indexes.Size() == 1) {
        std::shared_ptr<std::vector<uint>> result = std::make_shared<std::vector<uint>>();
        for (uint i = 0; i < indexes.GetRangeByIndex(0)->Size(); i++)
            result->push_back(indexes.GetRangeByIndex(0)->operator[](i));
        return result;
    }

    // Check if any index is empty => Intersection empty
    if (indexes.HasEmpty())
        return result_set;

    indexes.UpdateCurrentPostion();
    // 创建指向每一个列表的指针，初始指向列表的第一个值

    //  max 是所有指针指向位置的最大值，初始的最大值就是对列表排序后，最后一个列表的第一个值
    size_t max = indexes.GetCurrentValOfRange(indexes.Size() - 1);
    // 当前迭代器的 id
    size_t idx = 0;

    uint value;
    while (true) {
        // 当前迭代器的第一个值
        value = indexes.GetCurrentValOfRange(idx);

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
            indexes.NextVal(idx);
            // We shall find a value greater or equal than the current max
        } else {
            // 将当前迭代器指向的位置变为第一个大于 max 的值的位置
            indexes.Seek(idx, max);
        }

        if (indexes.AtEnd(idx)) {
            break;
        }

        // Store the maximum
        max = indexes.GetCurrentValOfRange(idx);

        idx++;
        idx = idx % indexes.Size();
    }

    return result_set;
}

bool QueryExecutor::PreJoin() {
    ResultList result_list;
    std::stringstream key;
    _pre_join_result = std::vector<std::shared_ptr<std::vector<uint>>>(stat_.plan_.size());
    for (long unsigned int level = 0; level < stat_.plan_.size(); level++) {
        if (level != 0 && (!none_type_[level].empty() || !prestores_[level].empty()))
            continue;
        if (level == 0 && prestores_[0].size())
            result_list.AddVectors(prestores_[0]);

        for (long unsigned int i = 0; i < stat_.plan_[level].size(); i++) {
            if (stat_.plan_[level][i].search_type_ != PlanGenerator::Item::TypeT::kNone) {
                result_list.AddVector(stat_.plan_[level][i].search_result_);
            }
        }
        if (result_list.Size() > 1) {
            _pre_join_result[level] = LeapfrogJoin(result_list);
            if (_pre_join_result[level]->size() == 0)
                return false;
        }
        result_list.Clear();
    }
    return true;
}

void QueryExecutor::Down(Stat& stat) {
    ++stat.level_;
    // sleep(2);

    // 如果当前层没有查询结果，就生成结果
    if (stat.candidate_result_[stat.level_]->empty()) {
        // check whether there are some have the Item::Type_T::None
        EnumerateItems(stat);
        if (stat.at_end_) {
            return;
        }
    }

    // 遍历当前level_所有经过连接的得到的结果实体
    // 并将这些实体添加到存储结果的 current_tuple_ 中

    bool success = UpdateCurrentTuple(stat);
    while (!success && !stat.at_end_) {
        success = UpdateCurrentTuple(stat);
    }
}

void QueryExecutor::Up(Stat& stat) {
    // 清除较高 level_ 的查询结果
    stat.candidate_result_[stat.level_]->clear();
    stat.indices_[stat.level_] = 0;

    --stat.level_;
}

void QueryExecutor::Next(Stat& stat) {
    // 当前 level_ 的下一个 candidate_result_
    stat.at_end_ = false;
    bool success = UpdateCurrentTuple(stat);
    while (!success && !stat.at_end_) {
        success = UpdateCurrentTuple(stat);
    }
}

void QueryExecutor::EnumerateItems(Stat& stat) {
    // 每一层可能有
    // 1.单变量三元组的查询结果，存储在 _prestore_result 中，
    // 2.双变量三元组的 none 类型的 item，查询结果search_result在之前的层数被填充在 plan 中，
    // 3.双变量三元组的非 none 类型的 item

    ResultList result_list;

    // _prestore_result 是 (?s p o) 和 (?s p o) 的查询结果
    if (!prestores_[stat.level_].empty()) {
        // join item for none type
        // 如果有此变量（level_）在单变量三元组中，且有查询结果，
        // 就将此变量在所有三元组中的查询结果插入到查询结果列表的末尾
        if (!result_list.AddVectors(prestores_[stat.level_])) {
            stat.at_end_ = true;
            return;
        }
    }

    // none 类型已经在之前的层数的时候就已经填补上了查询范围，
    // 而且 none 类型不可能在第 0 层
    for (const auto& idx : none_type_[stat.level_]) {
        // 将 none 类型的 item（查询结果）放入查询列表中
        result_list.AddVector(stat.plan_[stat.level_][idx].search_result_);
    }

    const auto& item_other_type_indices_ = other_type_[stat.level_];
    uint join_case = result_list.Size();

    if (join_case == 0 || stat_.level_ == 0) {
        if (_pre_join_result[stat_.level_] != nullptr) {
            stat.candidate_result_[stat.level_]->reserve(_pre_join_result[stat_.level_]->size());
            for (auto iter = _pre_join_result[stat_.level_]->begin();
                 iter != _pre_join_result[stat_.level_]->end(); iter++) {
                stat.candidate_result_[stat.level_]->emplace_back(std::move(*iter));
            }
            return;
        } else {
            for (const auto& idx : item_other_type_indices_) {
                result_list.AddVector(stat.plan_[stat.level_][idx].search_result_);
            }
        }
        stat.candidate_result_[stat.level_] = LeapfrogJoin(result_list);
    }

    if (join_case == 1 && stat_.level_ != 0) {
        // for (const auto& idx : item_other_type_indices_) {
        // result_list.AddVector(stat.plan_[stat.level_][idx].search_result_);
        // }
        // stat.candidate_result_[stat.level_] = LeapfrogJoin(result_list);
        std::shared_ptr<Result> range = result_list.GetRangeByIndex(0);
        stat.candidate_result_[stat.level_] = std::make_shared<std::vector<uint>>();
        for (uint i = 0; i < range->Size(); i++) {
            stat.candidate_result_[stat.level_]->push_back(range->operator[](i));
        }
    }
    if (join_case > 1) {
        stat.candidate_result_[stat.level_] = LeapfrogJoin(result_list);
    }

    // 变量的交集为空
    if (stat.candidate_result_[stat.level_]->empty()) {
        stat.at_end_ = true;
        return;
    }
}

bool QueryExecutor::UpdateCurrentTuple(Stat& stat) {
    // stat.indices_ 用于更新已经处理过的交集结果在结果列表中的 id
    size_t idx = stat.indices_[stat.level_];

    bool have_other_type = !other_type_[stat.level_].empty();
    if (idx < stat.candidate_result_[stat.level_]->size()) {
        // candidate_result_ 是每一个 level_ 交集的计算结果，
        // entity 是第 idx 个结果
        uint entity = stat.candidate_result_[stat.level_]->at(idx);

        // if also have the other type item(s), search predicate path for these item(s)
        if (!have_other_type) {
            stat.current_tuple_[stat.level_] = entity;
            ++idx;
            stat.indices_[stat.level_] = idx;
            return true;
        }

        // 填补当前level_上的变量所在的三元组的 none 类型 item 的查找范围
        // 只更新 candidate_result_ 里的符合查询条件的 none 类型
        if (SearchPredicatePath(stat, entity)) {
            stat.current_tuple_[stat.level_] = entity;
            ++idx;
            stat.indices_[stat.level_] = idx;
            return true;
        }

        // 如果不存在三元组 (?s, search_code, entity)，则表明 entity 不符合查询条件
        // 切换到下一个 entity
        ++idx;
        stat.indices_[stat.level_] = idx;
    } else {
        stat.at_end_ = true;
    }

    return false;
}

bool QueryExecutor::SearchPredicatePath(Stat& stat, uint entity) {
    bool match = true;
    // 遍历一个变量（level_）的在所有三元组中的查询结果
    for (auto& item : stat.plan_[stat.level_]) {
        if (item.search_type_ == PlanGenerator::Item::TypeT::kPS) {
            size_t other = item.candidate_result_idx_;

            // 遍历当前三元组none所在的level_
            for (auto& other_item : stat.plan_[other]) {
                if (other_item.search_code_ != item.search_code_) {
                    // 确保谓词相同，在一个三元组中
                    continue;
                }

                other_item.search_result_ = index_->GetByOP(entity, item.search_code_);
                if (other_item.search_result_->Size() == 0) {
                    match = false;
                }

                break;
            }
        } else if (item.search_type_ == PlanGenerator::Item::TypeT::kPO) {
            size_t other = item.candidate_result_idx_;

            for (auto& other_item : stat.plan_[other]) {
                if (other_item.search_code_ != item.search_code_) {
                    continue;
                }

                other_item.search_result_ = index_->GetBySP(entity, item.search_code_);
                if (other_item.search_result_->Size() == 0) {
                    match = false;
                }

                break;
            }
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