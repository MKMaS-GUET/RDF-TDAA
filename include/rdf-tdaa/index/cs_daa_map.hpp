#ifndef CS_DAA_MAP_HPP
#define CS_DAA_MAP_HPP

#include <sys/types.h>
#include <string>
#include <vector>
#include "rdf-tdaa/utils/mmap.hpp"

/**
 * @class CsDaaMap
 * @brief A class for mapping entity IDs to characteristic set IDs and DAA offsets.
 */
class CsDaaMap {
   public:
    /**
     * @enum Permutation
     * @brief Enum representing the permutation types for mappings.
     *
     * - kSPO: Subject-Predicate-Object permutation.
     * - kOPS: Object-Predicate-Subject permutation.
     */
    enum Permutation { kSPO, kOPS };

   private:
    // File path for the mapping data.
    std::string file_path_;

    // Memory-mapped data structure for storing characteristic set and DAA mappings.
    MMap<uint> cs_daa_map_;

    /**
     * @brief Pair representing the width of characteristic set IDs.
     * - First element: Width of subject characteristic set IDs.
     * - Second element: Width of object characteristic set IDs.
     */
    std::pair<ulong, ulong> cs_id_width_;

    /**
     * @brief Pair representing the width of DAA offsets.
     * - First element: Width of subject DAA offsets.
     * - Second element: Width of object DAA offsets.
     */
    std::pair<ulong, ulong> daa_offset_width_;

    // Width of shared entity's cs id width and daa offset width.
    ulong shared_width_;

    // WWidth of non-shared entity's cs id width and daa offset width.
    ulong not_shared_width_;

    // Width of non-shared characteristic set IDs.
    ulong not_shared_cs_id_width_;

    // Width of non-shared DAA offsets.
    ulong not_shared_daa_offset_width_;

    // Count of shared entities.
    ulong shared_cnt_;

    // Count of subjects.
    ulong subject_cnt_;

    // Count of objects.
    ulong object_cnt_;

    // Count of how many subject ids and object ids are the same.
    ulong shared_id_size_;

    /**
     * @brief Retrieves the DAA offset for a given ID and permutation.
     * @param id The ID for which the DAA offset are retrieved.
     * @param permutation The permutation type (kSPO or kOPS).
     * @return The DAA offset.
     */
    uint DAAOffsetOf(uint id, Permutation permutation);

   public:
    /**
     * @brief Default constructor for CsDaaMap.
     */
    CsDaaMap() = default;

    /**
     * @brief Constructor to initialize CsDaaMap with a file path.
     * @param file_path The file path for the mapping data.
     */
    CsDaaMap(std::string file_path);

    /**
     * @brief Constructor to initialize CsDaaMap with detailed parameters.
     * @param file_path File path for the mapping data.
     * @param cs_id_width Pair representing the width of characteristic set IDs.
     * @param daa_offset_width Pair representing the width of DAA offsets.
     * @param not_shared_cs_id_width Width of non-shared characteristic set IDs.
     * @param not_shared_daa_offset_width Width of non-shared DAA offsets.
     * @param shared_cnt Count of shared mappings.
     * @param subject_cnt Count of subjects.
     * @param object_cnt Count of objects.
     * @param shared_id_size Count of how many subject ids and object ids are the same.
     */
    CsDaaMap(std::string file_path,
             std::pair<uint, uint> cs_id_width,
             std::pair<uint, uint> daa_offset_width,
             uint not_shared_cs_id_width,
             uint not_shared_daa_offset_width,
             uint shared_cnt,
             uint subject_cnt,
             uint object_cnt,
             uint shared_id_size);

    /**
     * @brief Builds the CsDaaMap using SPO and OPS mappings.
     * @param spo_map Pair of vectors representing the SPO mapping.
     * @param ops_map Pair of vectors representing the OPS mapping.
     */
    void Build(std::pair<std::vector<uint>&, std::vector<ulong>&> spo_map,
               std::pair<std::vector<uint>&, std::vector<ulong>&> ops_map);

    /**
     * @brief Retrieves the characteristic set ID for a given ID and permutation.
     * @param id The ID for which the characteristic set ID is retrieved.
     * @param permutation The permutation type (kSPO or kOPS).
     * @return The characteristic set ID.
     */
    uint ChararisticSetIdOf(uint id, Permutation permutation);

    /**
     * @brief Retrieves the DAA offset and size for a given ID and permutation.
     * @param id The ID for which the DAA offset and size are retrieved.
     * @param permutation The permutation type (kSPO or kOPS).
     * @return A pair containing the DAA offset and size.
     */
    std::pair<uint, uint> DAAOffsetSizeOf(uint id, Permutation permutation);

    /**
     * @brief Retrieves the size of shared IDs.
     * @return The size of shared IDs.
     */
    uint shared_id_size();

    /**
     * @brief Retrieves the width of characteristic set IDs.
     * @return A pair representing the width of characteristic set IDs.
     */
    std::pair<uint, uint> cs_id_width();

    /**
     * @brief Retrieves the width of DAA offsets.
     * @return A pair representing the width of DAA offsets.
     */
    std::pair<uint, uint> daa_offset_width();

    /**
     * @brief Retrieves the width of non-shared characteristic set IDs.
     * @return The width of non-shared characteristic set IDs.
     */
    uint not_shared_cs_id_width();

    /**
     * @brief Retrieves the width of non-shared DAA offsets.
     * @return The width of non-shared DAA offsets.
     */
    uint not_shared_daa_offset_width();
};

#endif