#ifndef INDEX_RETRIEVER_HPP
#define INDEX_RETRIEVER_HPP

#include <limits.h>
#include <fstream>
#include <iostream>
#include <span>
#include <thread>
#include <vector>
#include "rdf-tdaa/dictionary/dictionary.hpp"
#include "rdf-tdaa/index/characteristic_set.hpp"
#include "rdf-tdaa/index/cs_daa_map.hpp"
#include "rdf-tdaa/index/daas.hpp"
#include "rdf-tdaa/index/predicate_index.hpp"

/**
 * @class IndexRetriever
 * @brief A class for retrieving RDF index data.
 */

class IndexRetriever {
    std::string db_path_;
    std::string db_dictionary_path_;
    std::string db_index_path_;
    std::string spo_index_path_;
    std::string ops_index_path_;

    Dictionary dict_;

    CharacteristicSet subject_characteristic_set_;
    CharacteristicSet object_characteristic_set_;
    CsDaaMap cs_daa_map_;
    DAAs spo_;
    DAAs ops_;

    PredicateIndex predicate_index_;

    ulong max_subject_id_;

    /**
     * @brief Retrieves the size of a file.
     * @param file_name The name of the file.
     * @return The size of the file in bytes.
     */
    ulong FileSize(std::string file_name);

   public:
    /**
     * @brief Default constructor for IndexRetriever.
     */
    IndexRetriever();

    /**
     * @brief Constructor for IndexRetriever with a database name.
     * @param db_name The name of the database.
     */
    IndexRetriever(std::string db_name);

    /**
     * @brief Closes the index retriever and releases resources.
     */
    void Close();

    /**
     * @brief Converts an ID to its corresponding string representation.
     * @param id The ID to convert.
     * @param pos The position of the term in the SPARQL triple pattern.
     * @return The string representation of the ID.
     */
    const char* ID2String(uint id, SPARQLParser::Term::Positon pos);

    /**
     * @brief Converts a term in a SPARQL triple pattern to its corresponding ID.
     * @param term The SPARQL term.
     * @return The ID of the term.
     */
    uint Term2ID(const SPARQLParser::Term& term);

    /**
     * @brief Retrieves the set of subjects for a given predicate ID.
     * @param pid The predicate ID.
     * @return A span of subject IDs.
     */
    std::span<uint> GetSSet(uint pid);

    /**
     * @brief Retrieves the set of objects for a given predicate ID.
     * @param pid The predicate ID.
     * @return A span of object IDs.
     */
    std::span<uint> GetOSet(uint pid);

    /**
     * @brief Retrieves the set of predicates for a given subject ID.
     * @param sid The subject ID.
     * @return A span of predicate IDs.
     */
    std::span<uint> GetSPreSet(uint sid);

    /**
     * @brief Retrieves the set of predicates for a given object ID.
     * @param oid The object ID.
     * @return A span of predicate IDs.
     */
    std::span<uint> GetOPreSet(uint oid);

    /**
     * @brief Retrieves the set of objects by subject ID.
     * @param sid The subject ID.
     * @return A span of triples.
     */
    std::span<uint> GetByS(uint sid);

    /**
     * @brief Retrieves the set of subjects by object ID.
     * @param oid The object ID.
     * @return A span of triples.
     */
    std::span<uint> GetByO(uint oid);

    /**
     * @brief Retrieves the set of objects by subject and predicate IDs.
     * @param sid The subject ID.
     * @param pid The predicate ID.
     * @return A span of triples.
     */
    std::span<uint> GetBySP(uint sid, uint pid);

    /**
     * @brief Retrieves the set of subjects by object and predicate IDs.
     * @param oid The object ID.
     * @param pid The predicate ID.
     * @return A span of triples.
     */
    std::span<uint> GetByOP(uint oid, uint pid);

    /**
     * @brief Retrieves the set of predicates by subject and object IDs.
     * @param sid The subject ID.
     * @param oid The object ID.
     * @return A span of triples.
     */
    std::span<uint> GetBySO(uint sid, uint oid);

    /**
     * @brief Retrieves the count of subjects for a given predicate ID.
     * @param pid The predicate ID.
     * @return The count of the subjects.
     */
    uint GetSSetSize(uint pid);

    /**
     * @brief Retrieves the count of objects for a given predicate ID.
     * @param pid The predicate ID.
     * @return The count of the objects.
     */
    uint GetOSetSize(uint pid);

    /**
     * @brief Retrieves the count of the objects by subject ID.
     * @param sid The subject ID.
     * @return The size of the triples.
     */
    uint GetBySSize(uint sid);

    /**
     * @brief Retrieves the count of the subjects by object ID.
     * @param oid The object ID.
     * @return The size of the triples.
     */
    uint GetByOSize(uint oid);

    /**
     * @brief Retrieves the count of the objects by subject and predicate IDs.
     * @param sid The subject ID.
     * @param pid The predicate ID.
     * @return The size of the triples.
     */
    uint GetBySPSize(uint sid, uint pid);

    /**
     * @brief Retrieves the count of the subjects by object and predicate IDs.
     * @param oid The object ID.
     * @param pid The predicate ID.
     * @return The size of the triples.
     */
    uint GetByOPSize(uint oid, uint pid);

    /**
     * @brief Retrieves the count of the predicates by subject and object IDs.
     * @param sid The subject ID.
     * @param oid The object ID.
     * @return The size of the triples.
     */
    uint GetBySOSize(uint sid, uint oid);

    /**
     * @brief Retrieves the count of predicates.
     * @return The count of predicates.
     */
    uint predicate_cnt();

    /**
     * @brief Retrieves the count of shared entities.
     * @return The count of shared resources.
     */
    uint shared_cnt();
};

#endif