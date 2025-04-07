#include <malloc.h>
#include <parallel_hashmap/btree.h>
#include <parallel_hashmap/phmap.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <thread>

#include "rdf-tdaa/utils/mmap.hpp"

template <typename Key, typename Value>
using hash_map = phmap::flat_hash_map<Key, Value>;

/**
 * @class DictionaryBuilder
 * @brief A class for building and managing RDF dictionaries.
 *
 * This class is responsible for creating dictionaries for RDF data, encoding RDF triples,
 * and managing the associated data structures. It uses hash maps to store subjects, predicates,
 * objects, and shared elements, and provides functionality to save and encode RDF data.
 */
class DictionaryBuilder {
    // Path to the dictionary file.
    std::string dict_path_;
    // Path to the RDF file.
    std::string file_path_;
    // Counter for the number of RDF triples loaded.
    ulong triplet_loaded_ = 0;
    // Contains data about the dictionary.
    MMap<ulong> menagement_data_;

    // subject -> id
    hash_map<std::string, uint> subjects_;
    // predicate -> id
    hash_map<std::string, uint> predicates_;
    // object -> id
    hash_map<std::string, uint> objects_;
    // shared -> id
    // shared: entities that are both subject and object.
    hash_map<std::string, uint> shared_;

    /**
     * @brief Initializes the dictionary builder.
     *
     * This private function sets up the necessary data structures and prepares the builder
     * for processing RDF data.
     */
    void Init();

    /**
     * @brief Builds the dictionary from RDF data.
     *
     * This private function processes the RDF file and populates the hash maps with the
     * necessary data.
     */
    void BuildDict();

    /**
     * @brief Reassigns IDs and saves the hash map to a file.
     *
     * @param map The hash map to be processed.
     * @param dict_out The output file stream for saving the dictionary.
     * @param hashmap_path The path where the hash map will be saved.
     * @param management_file_offset The offset in the management file for this hash map.
     */
    void ReassignIDAndSave(hash_map<std::string, uint>& map,
                           std::ofstream& dict_out,
                           std::string hashmap_path,
                           uint management_file_offset);

    /**
     * @brief Saves the dictionary using multiple threads.
     *
     * @param max_threads The maximum number of threads to use for saving the dictionary.
     */
    void SaveDict(uint max_threads);

   public:
    /**
     * @brief Constructor for the DictionaryBuilder class.
     *
     * @param dict_path The path where the dictionary will be stored.
     * @param file_path The path to the RDF file to be processed.
     */
    DictionaryBuilder(std::string& dict_path, std::string& file_path);

    /**
     * @brief Builds the RDF dictionary.
     *
     * This function processes the RDF data, populates the hash maps with subjects, predicates,
     * objects, and shared elements, and saves the dictionary to the specified path.
     */
    void Build();

    /**
     * @brief Encodes RDF triples into a hash map.
     *
     * @param pso A hash map where the key is a predicate ID, and the value is a vector of
     *            pairs containing subject and object IDs.
     */
    void EncodeRDF(hash_map<uint, std::vector<std::pair<uint, uint>>>& pso);

    /**
     * @brief Closes the dictionary builder and releases resources.
     *
     * This function ensures that all resources used by the dictionary builder are properly
     * released and any remaining data is saved.
     */
    void Close();
};
