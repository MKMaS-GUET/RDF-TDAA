#include "rdf-tdaa/parser/sparql_lexer.hpp"

SPARQLLexer::SPARQLLexer(std::string raw_sparql_string)
    : raw_sparql_string_(std::move(raw_sparql_string)),
      current_pos_(raw_sparql_string_.begin()),
      token_start_pos_(current_pos_),
      token_stop_pos_(current_pos_),
      PutBack_(TokenT::kNone),
      is_token_finish_(false) {}

SPARQLLexer::TokenT SPARQLLexer::GetNextTokenType() {
    if (is_token_finish_)
        return TokenT::kNone;

    if (PutBack_ != TokenT::kNone) {
        auto ret_value = PutBack_;
        PutBack_ = TokenT::kNone;
        return ret_value;
    }

    while (HasNext()) {
        is_token_finish_ = false;
        token_start_pos_ = current_pos_;
        switch (*(current_pos_++)) {
            case ' ':
            case '\n':
            case '\r':
            case '\f':
            case '\t':
                continue;
            case '{':
                token_stop_pos_ = current_pos_;
                return TokenT::kLCurly;
            case '}':
                token_stop_pos_ = current_pos_;
                return TokenT::kRCurly;
            case '(':
                token_stop_pos_ = current_pos_;
                return TokenT::kLRound;
            case ')':
                token_stop_pos_ = current_pos_;
                return TokenT::kRRound;
            case '.':
                token_stop_pos_ = current_pos_;
                return TokenT::kDot;
            case ':':
                token_stop_pos_ = current_pos_;
                return TokenT::kColon;
            case ';':
                token_stop_pos_ = current_pos_;
                return TokenT::kSemicolon;
            case ',':
                token_stop_pos_ = current_pos_;
                return TokenT::kComma;
            case '_':
                token_stop_pos_ = current_pos_;
                return TokenT::kUnderscore;
            case '@':
                token_stop_pos_ = current_pos_;
                return TokenT::kAt;
                //                case '+':
                //                    token_stop_pos_ = current_pos_;
                //                    return TokenT::kPlus;
                //                case '-':
                //                    token_stop_pos_ = current_pos_;
                //                    return TokenT::kMinus;
                //                case '/':
                //                    token_stop_pos_ = current_pos_;
                //                    return TokenT::kDiv;
            case '*':
                token_stop_pos_ = current_pos_;
                //                    return TokenT::kMul;
                return TokenT::kVariable;
            case '=':
                token_stop_pos_ = current_pos_;
                return TokenT::kEqual;
            case '!':
                if (*current_pos_ == '=') {
                    token_stop_pos_ = current_pos_;
                    return TokenT::kNotEqual;
                }
                is_token_finish_ = true;
                return TokenT::kError;
            case '>':
                if (*current_pos_ == '=') {
                    ++current_pos_;
                    token_stop_pos_ = current_pos_;
                    return TokenT::kGreaterOrEq;
                }
                token_stop_pos_ = current_pos_;
                return TokenT::kGreater;
            case '<':
                if (*current_pos_ == '=') {
                    ++current_pos_;
                    token_stop_pos_ = current_pos_;
                    return TokenT::kLessOrEq;
                } else if (*current_pos_ == ' ') {
                    token_stop_pos_ = current_pos_;
                    return TokenT::kLess;
                }
                while (HasNext() && IsLegalIRIInnerCharacter(*current_pos_)) {
                    if (*(current_pos_++) == '>') {
                        token_stop_pos_ = current_pos_;
                        return TokenT::kIRI;
                    }
                }
                is_token_finish_ = true;
                return TokenT::kError;
            case '"':
                while (HasNext()) {
                    if (*(current_pos_++) == '"') {
                        token_stop_pos_ = current_pos_;
                        return TokenT::kString;
                    }
                }
                is_token_finish_ = true;
                return TokenT::kError;
            case '?':
                while (HasNext() && IsLegalVariableCharacter(*current_pos_)) {
                    ++current_pos_;
                }
                token_stop_pos_ = current_pos_;
                return TokenT::kVariable;
            case '0':
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            case '9':
                // 整数部分
                while (HasNext() && IsLegalNumericalCharacter(*current_pos_)) {
                    ++current_pos_;
                }
                if (*current_pos_ == '.') {
                    ++current_pos_;
                    // 小数部分
                    while (HasNext() && IsLegalNumericalCharacter(*current_pos_)) {
                        ++current_pos_;
                    }
                }
                token_stop_pos_ = current_pos_;
                return TokenT::kNumber;
            default:
                // Identifier：1.关键字 2.用户自定义变量
                while (HasNext() && IsLegalIdentifierCharacter(*current_pos_)) {
                    ++current_pos_;
                }
                token_stop_pos_ = current_pos_;
                return TokenT::kIdentifier;
        }
    }
    return TokenT::kIdentifier;
}

std::string SPARQLLexer::GetCurrentTokenValue() const {
    return std::string(token_start_pos_, token_stop_pos_);
}

std::string SPARQLLexer::GetIRIValue() const {
    auto current_token_iter = token_start_pos_;
    std::string iri_value;
    for (; current_token_iter != token_stop_pos_; current_token_iter++) {
        char c = *current_token_iter;
        if (c == '\\') {
            if ((++current_token_iter) == token_stop_pos_) {
                break;
            }
            c = *current_token_iter;
        }
        iri_value += c;
    }
    return iri_value;
}

bool SPARQLLexer::IsKeyword(const char* word) {
    bool is_matched = true;
    auto current_token_iter = token_start_pos_;
    char* ch = const_cast<char*>(word);
    while (current_token_iter != token_stop_pos_) {
        if (*ch == '\0')
            break;
        char curr = *current_token_iter;
        // 大写转小写
        if ('A' <= curr && curr <= 'Z')
            curr += 'a' - 'A';
        if (curr != *ch) {
            is_matched = false;
            break;
        }
        ++current_token_iter;
        ++ch;
    }
    if (current_token_iter != token_stop_pos_ || *ch != '\0') {
        is_matched = false;
    }
    return is_matched;
}

void SPARQLLexer::PutBack(TokenT value) {
    PutBack_ = value;
}