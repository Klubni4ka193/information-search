#pragma once
#include <string>

class RussianStemmer {
 public:
  std::string stem(const std::string& token) const;

 private:
  static bool ends_with(const std::string& s, const std::string& suf);
  static int utf8_len_chars(const std::string& s);
  static bool looks_russian(const std::string& s);

  static bool remove_longest_suffix(std::string& s, const char* const* sufs, int n);
  std::string stem_one(const std::string& token) const;
};