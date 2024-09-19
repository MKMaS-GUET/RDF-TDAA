#ifndef SPARQL_PARSER_HPP
#define SPARQL_PARSER_HPP

#include <stdint.h>
#include <exception>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

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

    struct Term {
        enum Type { kVariable, kIRI, kLiteral, kBlank };
        enum ValueType { kInteger, kDouble, kString, kFunction, kNone };
        enum Positon { kSubject, kPredicate, kObject, kShared };

        Type type_;
        ValueType literal_type_;
        std::string value_;
        Positon position_;

        Term();

        Term(Type type, ValueType value_type, std::string value);

        bool IsVariable() const;
    };

    struct TriplePattern {
        Term subject_;
        Term predicate_;
        Term object_;
        bool is_option_;
        uint variale_cnt_;

        TriplePattern(Term subject, Term predicate, Term object, bool is_option, uint variale_cnt);

        TriplePattern(Term subject, Term predicate, Term object);
    };

    // TODO: Filter need to be imporoved
    struct Filter {
        enum Type { Equal, NotEqual, Less, LessOrEq, Greater, GreaterOrEq, Function };
        Type filter_type_;
        std::string variable_str_;
        // if Type == Function then filter_args[0] is functions_register_name
        std::vector<Term> filter_args_;
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

    Term MakeVariable(std::string variable);

    Term MakeIRI(std::string IRI);

    Term MakeIntegerLiteral(std::string literal);

    Term MakeDoubleLiteral(std::string literal);

    Term MakeNoTypeLiteral(std::string literal);

    Term MakeStringLiteral(const std::string& literal);

    Term MakeFunctionLiteral(std::string literal);

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
