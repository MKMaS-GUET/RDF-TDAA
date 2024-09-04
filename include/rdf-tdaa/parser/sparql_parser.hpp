#ifndef SPARQL_PARSER_HPP
#define SPARQL_PARSER_HPP

#include <exception>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <set>
#include <vector>
#include <stdint.h>

#include "rdf-tdaa/parser/sparql_lexer.hpp"

class SPARQLParser {
   public:
    struct ParserException : public std::exception {
        std::string message_;

        explicit ParserException(std::string message);

        explicit ParserException(const char* message);

        [[nodiscard]] const char* what() const noexcept override;

        [[nodiscard]] std::string to_string() const;
    };

    struct TriplePatternElem {
        enum Type { Variable, IRI, Literal, Blank };
        enum LiteralType { Integer, Double, String, Function, None };

        Type type_;
        LiteralType literal_type_;
        std::string value_;

        TriplePatternElem();

        TriplePatternElem(Type type, LiteralType literal_type, std::string value);
    };

    struct TriplePattern {
        TriplePatternElem subj_;
        TriplePatternElem pred_;
        TriplePatternElem obj_;
        bool is_option_;

        TriplePattern(TriplePatternElem subj, TriplePatternElem pred, TriplePatternElem obj, bool is_option);

        TriplePattern(TriplePatternElem subj, TriplePatternElem pred, TriplePatternElem obj);
    };

    // TODO: Filter need to be imporoved
    struct Filter {
        enum Type { Equal, NotEqual, Less, LessOrEq, Greater, GreaterOrEq, Function };
        Type filter_type_;
        std::string variable_str_;
        // if Type == Function then filter_args[0] is functions_register_name
        std::vector<TriplePatternElem> filter_args_;
    };

    struct ProjectModifier {
        enum Type { None, Distinct, Reduced, Count, Duplicates };

        Type modifier_type_;

        ProjectModifier(Type modifierType);

        [[nodiscard]] std::string toString() const;
    };

   private:
    size_t limit_;  // limit number
    SPARQLLexer sparql_lexer_;
    ProjectModifier project_modifier_;            // modifier
    std::vector<std::string> project_variables_;  // all variables to be outputted
    std::vector<TriplePattern> triple_patterns_;  // all triple patterns
    std::unordered_map<std::string, Filter> filters_;
    std::unordered_map<std::string, std::string> prefixes_;  // the registered prefixes

    void parse();

    void ParsePrefix();

    void ParseProjection();

    void ParseWhere();

    void ParseFilter();

    void ParseGroupGraphPattern();

    void ParseBasicGraphPattern(bool is_option);

    void ParseLimit();

    TriplePatternElem MakeVariable(std::string variable);

    TriplePatternElem MakeIRI(std::string IRI);

    TriplePatternElem MakeIntegerLiteral(std::string literal);

    TriplePatternElem MakeDoubleLiteral(std::string literal);

    TriplePatternElem MakeNoTypeLiteral(std::string literal);

    TriplePatternElem MakeStringLiteral(const std::string& literal);

    TriplePatternElem MakeFunctionLiteral(std::string literal);

   public:
    explicit SPARQLParser(const SPARQLLexer& sparql_lexer);

    explicit SPARQLParser(std::string input_string);

    ~SPARQLParser() = default;

    ProjectModifier project_modifier() const;

    const std::vector<std::string>& ProjectVariables() const;

    const std::vector<TriplePattern>& TriplePatterns() const;

    std::vector<std::vector<std::string>> TripleList() const;

    const std::unordered_map<std::string, Filter>& Filters() const;

    const std::unordered_map<std::string, std::string>& Prefixes() const;

    size_t Limit() const;
};

#endif  // SPARQL_PARSER_HPP
