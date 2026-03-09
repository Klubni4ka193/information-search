#pragma once
#include <string>
#include <vector>

struct TokenizerConfig {
  bool lowercase = true;
  bool normalize_yo = true; 
  bool keep_numbers = true;
  int min_len = 2;           
};

class Tokenizer {
 public:
  explicit Tokenizer(const TokenizerConfig& cfg) : cfg_(cfg) {}

  void tokenize(const std::string& text, std::vector<std::string>& out_tokens) const;

 private:
  TokenizerConfig cfg_;

  static bool is_ascii_letter(unsigned char c);
  static bool is_ascii_digit(unsigned char c);
  static bool is_dash(unsigned char c);
  static bool is_apos(unsigned char c);

  static bool is_cyrillic_pair(unsigned char c1, unsigned char c2);
  static void to_lower_cyrillic_pair(unsigned char& c1, unsigned char& c2);
  static void normalize_yo_pair(unsigned char& c1, unsigned char& c2);

  static int utf8_len_chars(const std::string& s);
};