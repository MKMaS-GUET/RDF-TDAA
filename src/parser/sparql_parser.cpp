#include "rdf-tdaa/parser/sparql_parser.hpp"
#include <codecvt>
#include <iomanip>
#include <iostream>

SPARQLParser::ParserException::ParserException(std::string message) : message(std::move(message)) {}

SPARQLParser::ParserException::ParserException(const char* message) : message(message) {}

const char* SPARQLParser::ParserException::what() const noexcept {
    return message.c_str();
}

std::string SPARQLParser::ParserException::to_string() const {
    return message;
}

SPARQLParser::Term::Term() : type(Type::kBlank), literal_type(ValueType::kNone), value() {}

SPARQLParser::Term::Term(Type type, ValueType literal_type, std::string value)
    : type(type), literal_type(literal_type), value(std::move(value)) {}

bool SPARQLParser::Term::IsVariable() const {
    return type == Type::kVariable;
}

SPARQLParser::TriplePattern::TriplePattern(Term subj, Term pred, Term obj, bool is_option, uint variale_cnt)
    : subject(std::move(subj)),
      predicate(std::move(pred)),
      object(std::move(obj)),
      is_option(is_option),
      variable_cnt(variale_cnt) {}

SPARQLParser::TriplePattern::TriplePattern(Term subj, Term pred, Term obj)
    : subject(std::move(subj)),
      predicate(std::move(pred)),
      object(std::move(obj)),
      is_option(false),
      variable_cnt(0) {}

SPARQLParser::ProjectModifier::ProjectModifier(Type modifierType) : modifier_type(modifierType) {}

std::string SPARQLParser::ProjectModifier::toString() const {
    switch (modifier_type) {
        case Type::None:
            return "Modifier::Type::None";
        case Type::Distinct:
            return "Modifier::Type::Distinct";
        case Type::Reduced:
            return "Modifier::Type::Reduced";
        case Type::Count:
            return "Modifier::Type::Count";
        case Type::Duplicates:
            return "Modifier::Type::Duplicates";
    }
    return "Modifier::Type::None";
}

void SPARQLParser::parse() {
    ParsePrefix();
    ParseProjection();
    ParseWhere();
    ParseGroupGraphPattern();
    ParseLimit();

    // 如果 select 后是 *，则查询结果的变量应该是三元组中出现的变量
    if (project_variables_[0] == "*") {
        project_variables_.clear();
        std::set<std::string> variables_set;
        for (const auto& item : triple_patterns_) {
            const auto& s = item.subject.value;
            const auto& p = item.predicate.value;
            const auto& o = item.object.value;
            if (s[0] == '?')
                variables_set.insert(s);
            if (p[0] == '?')
                variables_set.insert(p);
            if (o[0] == '?')
                variables_set.insert(o);
        }
        project_variables_.assign(variables_set.begin(), variables_set.end());
    }
}

void SPARQLParser::ParsePrefix() {
    for (;;) {
        auto token_t = sparql_lexer_.GetNextTokenType();
        if (token_t == SPARQLLexer::kIdentifier && sparql_lexer_.IsKeyword("prefix")) {
            if (sparql_lexer_.GetNextTokenType() != SPARQLLexer::kIdentifier) {
                throw ParserException("Expect : prefix name");
            }
            std::string name = sparql_lexer_.GetCurrentTokenValue();
            if (sparql_lexer_.GetNextTokenType() != SPARQLLexer::kColon) {
                throw ParserException("Expect : ':");
            }
            if (sparql_lexer_.GetNextTokenType() != SPARQLLexer::kIRI) {
                throw ParserException("Expect : IRI");
            }
            std::string iri = sparql_lexer_.GetIRIValue();
            if (prefixes_.count(name)) {
                throw ParserException("Duplicate prefix '" + name + "'");
            }
            prefixes_[name] = iri;
        } else {
            sparql_lexer_.PutBack(token_t);
            return;
        }
    }
}

void SPARQLParser::ParseProjection() {
    auto token_t = sparql_lexer_.GetNextTokenType();

    if (token_t != SPARQLLexer::TokenT::kIdentifier || !sparql_lexer_.IsKeyword("select")) {
        throw ParserException("Except : 'select'");
    }

    token_t = sparql_lexer_.GetNextTokenType();
    if (token_t == SPARQLLexer::kIdentifier) {
        if (sparql_lexer_.IsKeyword("distinct"))
            project_modifier_ = ProjectModifier::Type::Distinct;
        else if (sparql_lexer_.IsKeyword("reduced"))
            project_modifier_ = ProjectModifier::Type::Reduced;
        else if (sparql_lexer_.IsKeyword("count"))
            project_modifier_ = ProjectModifier::Type::Count;
        else if (sparql_lexer_.IsKeyword("duplicates"))
            project_modifier_ = ProjectModifier::Type::Duplicates;
        else
            sparql_lexer_.PutBack(token_t);
    } else
        sparql_lexer_.PutBack(token_t);

    project_variables_.clear();
    do {
        token_t = sparql_lexer_.GetNextTokenType();
        if (token_t != SPARQLLexer::TokenT::kVariable) {
            sparql_lexer_.PutBack(token_t);
            break;
        }
        project_variables_.push_back(sparql_lexer_.GetCurrentTokenValue());
    } while (true);

    if (project_variables_.empty()) {
        throw ParserException("project query_variables is empty");
    }
}

void SPARQLParser::ParseWhere() {
    if (sparql_lexer_.GetNextTokenType() != SPARQLLexer::TokenT::kIdentifier ||
        !sparql_lexer_.IsKeyword("where")) {
        throw ParserException("Except: 'where'");
    }
}

void SPARQLParser::ParseFilter() {
    if (sparql_lexer_.GetNextTokenType() != SPARQLLexer::TokenT::kLRound) {
        throw ParserException("Expect : (");
    }

    if (sparql_lexer_.GetNextTokenType() != SPARQLLexer::TokenT::kVariable) {
        throw ParserException("Expect : Variable");
    }
    std::string variable = sparql_lexer_.GetCurrentTokenValue();
    Filter filter;
    filter.variable_str = variable;
    auto token = sparql_lexer_.GetNextTokenType();
    switch (token) {
        case SPARQLLexer::TokenT::kEqual:
            filter.filter_type = Filter::Type::Equal;
            break;
        case SPARQLLexer::TokenT::kNotEqual:
            filter.filter_type = Filter::Type::NotEqual;
            break;
        case SPARQLLexer::TokenT::kLess:
            filter.filter_type = Filter::Type::Less;
            break;
        case SPARQLLexer::TokenT::kLessOrEq:
            filter.filter_type = Filter::Type::LessOrEq;
            break;
        case SPARQLLexer::TokenT::kGreater:
            filter.filter_type = Filter::Type::Greater;
            break;
        case SPARQLLexer::TokenT::kGreaterOrEq:
            filter.filter_type = Filter::Type::GreaterOrEq;
            break;
        default:
            filter.filter_type = Filter::Type::Function;
            std::string function_name = sparql_lexer_.GetCurrentTokenValue();
            filter.filter_args.push_back(MakeFunctionLiteral(function_name));
            break;
    }
    bool is_finish = false;
    while (!is_finish) {
        auto token = sparql_lexer_.GetNextTokenType();
        switch (token) {
            case SPARQLLexer::TokenT::kRRound:
                is_finish = true;
                break;
            case SPARQLLexer::TokenT::kEof:
                throw ParserException("Unexpect EOF in parse 'filter(...'");
            case SPARQLLexer::TokenT::kString: {
                std::string value = sparql_lexer_.GetCurrentTokenValue();
                auto string_elem = MakeStringLiteral(value);
                filter.filter_args.push_back(string_elem);
            } break;
            case SPARQLLexer::TokenT::kNumber: {
                std::string value = sparql_lexer_.GetCurrentTokenValue();
                auto double_elem = MakeDoubleLiteral(value);
                filter.filter_args.push_back(double_elem);
            } break;
            default:
                throw ParserException("Parse filter failed when meet :" +
                                      sparql_lexer_.GetCurrentTokenValue());
        }
    }
    filters_[filter.variable_str] = filter;
}

void SPARQLParser::ParseGroupGraphPattern() {
    if (sparql_lexer_.GetNextTokenType() != SPARQLLexer::TokenT::kLCurly) {
        throw ParserException("Except : '{'");
    }

    while (true) {
        auto token_t = sparql_lexer_.GetNextTokenType();
        if (token_t == SPARQLLexer::TokenT::kLCurly) {
            sparql_lexer_.PutBack(token_t);
            ParseGroupGraphPattern();
        } else if (token_t == SPARQLLexer::TokenT::kIdentifier && sparql_lexer_.IsKeyword("optional") &&
                   sparql_lexer_.IsKeyword("OPTIONAL")) {
            if (sparql_lexer_.GetNextTokenType() != SPARQLLexer::TokenT::kLCurly) {
                throw ParserException("Except : '{'");
            }
            ParseBasicGraphPattern(true);
            if (sparql_lexer_.GetNextTokenType() != SPARQLLexer::TokenT::kRCurly) {
                throw ParserException("Except : '}'");
            }
        } else if (token_t == SPARQLLexer::TokenT::kIdentifier && sparql_lexer_.IsKeyword("filter") &&
                   sparql_lexer_.IsKeyword("FILTER")) {
            ParseFilter();
        } else if (token_t == SPARQLLexer::TokenT::kRCurly) {
            break;
        } else if (token_t == SPARQLLexer::TokenT::kEof) {
            throw ParserException("Unexpect EOF");
        } else {
            sparql_lexer_.PutBack(token_t);
            ParseBasicGraphPattern(false);
        }
    }
}

void SPARQLParser::ParseBasicGraphPattern(bool is_option) {
    Term pattern_term[3];
    uint variable_cnt = 0;
    for (uint i = 0; i < 3; ++i) {
        auto token_t = sparql_lexer_.GetNextTokenType();
        std::string token_value = sparql_lexer_.GetCurrentTokenValue();

        std::wstring_convert<std::codecvt_utf8<wchar_t>> converter;
        std::wstring w_token_value = converter.from_bytes(token_value);

        std::ostringstream oss;
        for (wchar_t wc : w_token_value) {
            if (wc < 0x80)
                oss << (char)wc;
            else
                oss << "\\u" << std::uppercase << std::hex << std::setw(4) << std::setfill('0') << (int)wc;
        }
        token_value = oss.str();

        Term term;
        switch (token_t) {
            case SPARQLLexer::TokenT::kVariable:
                term = MakeVariable(token_value);
                variable_cnt++;
                break;
            case SPARQLLexer::TokenT::kIRI:
                term = MakeIRI(token_value);
                break;
            case SPARQLLexer::TokenT::kString:
                term = MakeStringLiteral(token_value);
                break;
            case SPARQLLexer::TokenT::kNumber:
                term = MakeDoubleLiteral(token_value);
                break;
            case SPARQLLexer::TokenT::kIdentifier:
                term = MakeNoTypeLiteral(token_value);
                break;
            default:
                throw ParserException("Except variable or IRI or Literal or Blank");
        }
        term.position = SPARQLParser::Term::Positon(i);
        pattern_term[i] = term;
    }
    auto token_t = sparql_lexer_.GetNextTokenType();
    if (token_t != SPARQLLexer::TokenT::kDot) {
        sparql_lexer_.PutBack(token_t);
    }
    TriplePattern pattern(pattern_term[0], pattern_term[1], pattern_term[2], is_option, variable_cnt);
    triple_patterns_.push_back(std::move(pattern));
}

void SPARQLParser::ParseLimit() {
    auto token_t = sparql_lexer_.GetNextTokenType();
    if (token_t == SPARQLLexer::kIdentifier && sparql_lexer_.IsKeyword("limit")) {
        if (sparql_lexer_.GetNextTokenType() != SPARQLLexer::kNumber) {
            throw ParserException("Except : limit number");
        }
        std::string curr_number_token = sparql_lexer_.GetCurrentTokenValue();
        std::stringstream ss(curr_number_token);
        ss >> limit_;
    } else {
        sparql_lexer_.PutBack(token_t);
    }
}

SPARQLParser::Term SPARQLParser::MakeVariable(std::string variable) {
    return {Term::Type::kVariable, Term::ValueType::kNone, std::move(variable)};
}

SPARQLParser::Term SPARQLParser::MakeIRI(std::string IRI) {
    return {Term::Type::kIRI, Term::ValueType::kNone, std::move(IRI)};
}

SPARQLParser::Term SPARQLParser::MakeIntegerLiteral(std::string literal) {
    return {Term::Type::kLiteral, Term::ValueType::kInteger, std::move(literal)};
}

SPARQLParser::Term SPARQLParser::MakeDoubleLiteral(std::string literal) {
    return {Term::Type::kLiteral, Term::ValueType::kDouble, std::move(literal)};
}

SPARQLParser::Term SPARQLParser::MakeNoTypeLiteral(std::string literal) {
    return {Term::Type::kLiteral, Term::ValueType::kNone, std::move(literal)};
}

SPARQLParser::Term SPARQLParser::MakeStringLiteral(const std::string& literal) {
    // size_t literal_len = literal.size();
    // std::string cleaned_literal;
    // if (literal_len > 2) {
    //     cleaned_literal = literal.substr(1, literal_len - 2);
    // }
    // return {Term::Type::kLiteral, Term::ValueType::kString, cleaned_literal};
    return {Term::Type::kLiteral, Term::ValueType::kString, literal};
}

SPARQLParser::Term SPARQLParser::MakeFunctionLiteral(std::string literal) {
    return {Term::Type::kLiteral, Term::ValueType::kFunction, std::move(literal)};
}

SPARQLParser::SPARQLParser(const SPARQLLexer& sparql_lexer)
    : limit_(UINTMAX_MAX), sparql_lexer_(sparql_lexer), project_modifier_(ProjectModifier::Type::None) {
    parse();
}

SPARQLParser::SPARQLParser(std::string input_string)
    : limit_(UINTMAX_MAX),
      sparql_lexer_(SPARQLLexer(std::move(input_string))),
      project_modifier_(ProjectModifier::Type::None) {
    parse();
}

SPARQLParser::ProjectModifier SPARQLParser::project_modifier() const {
    return project_modifier_;
}

const std::vector<std::string>& SPARQLParser::ProjectVariables() const {
    return project_variables_;
}

const std::vector<SPARQLParser::TriplePattern>& SPARQLParser::TriplePatterns() const {
    return triple_patterns_;
}

std::vector<std::vector<std::string>> SPARQLParser::TripleList() const {
    std::vector<std::vector<std::string>> list;
    for (const auto& item : triple_patterns_) {
        list.push_back({item.subject.value, item.predicate.value, item.object.value});
    }
    return list;
}

const std::unordered_map<std::string, SPARQLParser::Filter>& SPARQLParser::Filters() const {
    return filters_;
}

const std::unordered_map<std::string, std::string>& SPARQLParser::Prefixes() const {
    return prefixes_;
}

size_t SPARQLParser::Limit() const {
    return limit_;
}