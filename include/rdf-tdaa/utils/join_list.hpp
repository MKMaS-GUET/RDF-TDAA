#ifndef JOIN_LIST_HPP
#define JOIN_LIST_HPP

#include <iterator>
#include <memory>
#include <span>
#include <string>
#include <vector>

/**
 * @class JoinList
 * @brief A utility class for managing and performing operations on a collection of lists to support Leapfrog join.
 *
 * This class provides functionality to manage multiple lists (represented as spans of unsigned integers),
 * perform operations such as finding the shortest list, updating positions, seeking values, and more.
 */
class JoinList {
    /**
     * @brief A vector that holds a collection of spans, where each span represents
     *        a sorted list.
     */
    std::vector<std::span<uint>> lists_;

    /**
     * @brief A vector storing iterators pointing to the current positions
     *        within lists in lists_.
     */
    std::vector<std::span<uint>::iterator> list_current_pos_;

   public:
    JoinList();

    JoinList(std::vector<std::span<uint>>& lists);

    /**
     * @brief Adds a single list to the collection according to its first value.
     * @param list A span representing the list to be added.
     */
    void AddList(const std::span<uint>& list);

    /**
     * @brief Adds multiple lists to the collection.
     * @param lists A vector of spans representing the lists to be added.
     */
    void AddLists(const std::vector<std::span<uint>>& lists);

    /**
     * @brief Updates the current positions in list_current_pos_ of all lists in the collection.
     */
    void UpdateCurrentPostion();

    /**
     * @brief Moves the current position of i-th list to the first value that bigger then val.
     * @param i The index of the list to seek in.
     * @param val The value to seek.
     */
    void Seek(int i, uint val);

    /**
     * @brief Retrieves the current value of the i-th list.
     * @param i The index of the list.
     * @return The first value of the i-th list after sorting.
     */
    uint GetCurrentValOfList(int i);

    /**
     * @brief Advances the current position of the i-th list to the next value.
     * @param i The index of the list to update.
     */
    void NextVal(int i);

    /**
     * @brief Retrieves a list by its index.
     * @param i The index of the list to retrieve.
     * @return A span representing the list at the specified index.
     */
    std::span<uint> GetListByIndex(int i);

    /**
     * @brief Checks if any list in the collection is empty.
     * @return True if at least one list is empty, false otherwise.
     */
    bool HasEmpty();

    /**
     * @brief Checks if the iterator of the i-th list has reached the end.
     * @param i The index of the list to check.
     * @return True if the iterator is at the end, false otherwise.
     */
    bool AtEnd(int i);

    /**
     * @brief Clears all lists from the collection.
     */
    void Clear();

    /**
     * @brief Retrieves the number of lists in the collection.
     * @return The number of lists.
     */
    int Size();
};

#endif