#ifndef SPARQL_LEXER_HPP
#define SPARQL_LEXER_HPP

#include <string>

class SPARQLLexer {
   public:
    enum TokenT {
        kNone,
        kError,
        kEof,
        kVariable,
        kIRI,
        kIdentifier,
        kColon,
        kSemicolon,
        kComma,
        kUnderscore,
        kAt,
        kPlus,
        kMinus,
        kMul,
        kDiv,
        kString,
        kNumber,
        kDot,
        kLCurly,
        kRCurly,
        kLRound,
        kRRound,
        kEqual,
        kNotEqual,
        kLess,
        kLessOrEq,
        kGreater,
        kGreaterOrEq
    };

   private:
    std::string raw_sparql_string_;
    std::string::const_iterator current_pos_;
    // token 的第一个字符位置
    std::string::const_iterator token_start_pos_;
    // token 之后的第一个字符
    std::string::const_iterator token_stop_pos_;
    TokenT PutBack_;
    bool is_token_finish_;

    inline bool IsLegalNumericalCharacter(const char& curr) { return '0' <= curr && curr <= '9'; }

    inline bool IsLegalIdentifierCharacter(const char& curr) {
        return ('0' <= curr && curr <= '9') || ('a' <= curr && curr <= 'z') || ('A' <= curr && curr <= 'Z') ||
               ('_' == curr);
    }

    inline bool IsLegalVariableCharacter(const char& curr) { return IsLegalIdentifierCharacter(curr); }

    inline bool IsLegalIRIInnerCharacter(const char& curr) {
        return '\t' != curr && ' ' != curr && '\n' != curr && '\r' != curr;
    }

   public:
    explicit SPARQLLexer(std::string raw_sparql_string);

    ~SPARQLLexer() = default;

    inline bool HasNext() { return token_stop_pos_ != raw_sparql_string_.end(); }

    TokenT GetNextTokenType();

    [[nodiscard]] std::string GetCurrentTokenValue() const;

    [[nodiscard]] std::string GetIRIValue() const;

    bool IsKeyword(const char* word);

    // 当下一次调用 GetNextTokenType 时，会直接返回 PutBack_
    void PutBack(TokenT value);
};

#endif  // SPARQL_LEXER_HPP
