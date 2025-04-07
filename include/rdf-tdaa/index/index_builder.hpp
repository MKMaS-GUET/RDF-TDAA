#ifndef INDEX_BUILDER_HPP
#define INDEX_BUILDER_HPP

#include <parallel_hashmap/btree.h>
#include <parallel_hashmap/phmap.h>
#include <filesystem>
#include <future>
#include <iostream>
#include <mutex>
#include <queue>
#include <string>
#include <vector>

#include "rdf-tdaa/dictionary/dictionary.hpp"
#include "rdf-tdaa/index/daas.hpp"
#include "rdf-tdaa/index/predicate_index.hpp"

namespace fs = std::filesystem;

/**
 * @class IndexBuilder
 * @brief A class for building RDF data indexes and dictionaries.
 *
 * The IndexBuilder class provides methods to build characteristic sets, entity sets,
 * and manage RDF data indexing. It supports multiple permutations (e.g., SPO, OPS)
 * and includes options for compressing predicate indexes and converting them to DAA.
 */

class IndexBuilder {
    /**
     * @enum Permutation
     * @brief Enum representing the supported RDF triple permutations.
     *
     * The permutations define the order of RDF triple components:
     * - kSPO: Subject-Predicate-Object
     * - kOPS: Object-Predicate-Subject
     */
    enum class Permutation { kSPO, kOPS };

    // Path to the RDF data file.
    std::string data_file_;
    // Path to the database index file.
    std::string db_index_path_;
    // Path to the database dictionary file.
    std::string db_dictionary_path_;
    // Path to the Subject-Predicate-Object (SPO) index file.
    std::string spo_index_path_;
    // Path to the Object-Predicate-Subject (OPS) index file.
    std::string ops_index_path_;
    // Name of the database.
    std::string db_name_;

    // Dictionary
    Dictionary dict_;

    // p_id -> (s_id, o_id).
    std::shared_ptr<hash_map<uint, std::vector<std::pair<uint, uint>>>> pso_;

    /**
     * @brief Builds the characteristic set index for the given permutation.
     * @param c_set_id A vector to store characteristic set IDs.
     * @param c_set_size A vector to store size for characteristic sets.
     * @param permutation The RDF triple permutation to build.
     */
    void BuildCharacteristicSet(std::vector<uint>& c_set_id,
                                std::vector<uint>& c_set_size,
                                Permutation permutation);

    /**
     * @brief Builds the entity sets for the given permutation.
     * @param predicate_index The predicate index to use.
     * @param c_set_size A vector to store size for characteristic sets.
     * @param entity_set entity -> predicate_offset -> o/s set.
     * @param permutation The RDF triple permutation to build.
     */
    void BuildEntitySets(PredicateIndex& predicate_index,
                         std::vector<uint>& c_set_size,
                         std::vector<std::vector<std::vector<uint>>>& entity_set,
                         Permutation permutation);

   public:
    /**
     * @brief Constructs an IndexBuilder object.
     * @param db_name The name of the database.
     * @param data_file The path to the RDF data file.
     */
    IndexBuilder(std::string db_name, std::string data_file);

    /**
     * @brief Builds the RDF indexes and dictionaries.
     * @return True if the build process is successful, false otherwise.
     */
    bool Build();
};

#endif