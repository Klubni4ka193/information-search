#include "stemmer.h"
#include "util.h"

static inline unsigned char uc(char c) { return (unsigned char)c; }

bool RussianStemmer::ends_with(const std::string& s, const std::string& suf) {
  if (s.size() < suf.size()) return false;
  return s.compare(s.size() - suf.size(), suf.size(), suf) == 0;
}

int RussianStemmer::utf8_len_chars(const std::string& s) {
  int n = 0;
  for (size_t i = 0; i < s.size();) {
    unsigned char c = uc(s[i]);
    if (c < 0x80) { i += 1; n += 1; }
    else if (i + 1 < s.size()) { i += 2; n += 1; }
    else break;
  }
  return n;
}

bool RussianStemmer::looks_russian(const std::string& s) {
  for (size_t i = 0; i + 1 < s.size(); ++i) {
    unsigned char c1 = uc(s[i]);
    unsigned char c2 = uc(s[i + 1]);
    (void)c2;
    if (c1 == 0xD0 || c1 == 0xD1) return true;
  }
  return false;
}

bool RussianStemmer::remove_longest_suffix(std::string& s, const char* const* sufs, int n) {
  int best = -1;
  int best_len = 0;
  for (int i = 0; i < n; ++i) {
    std::string suf = sufs[i];
    if (ends_with(s, suf)) {
      if ((int)suf.size() > best_len) {
        best_len = (int)suf.size();
        best = i;
      }
    }
  }
  if (best >= 0) {
    s.resize(s.size() - (size_t)best_len);
    return true;
  }
  return false;
}

std::string RussianStemmer::stem(const std::string& token) const {
  if (token.empty()) return token;

  size_t pos = token.find('-');
  if (pos != std::string::npos) {
    std::vector<std::string> parts;
    split_by_char(token, '-', parts);
    std::string out;
    for (size_t i = 0; i < parts.size(); ++i) {
      if (i) out.push_back('-');
      out += stem_one(parts[i]);
    }
    return out;
  }
  return stem_one(token);
}

std::string RussianStemmer::stem_one(const std::string& token) const {
  if (!looks_russian(token)) return token;
  if (utf8_len_chars(token) <= 3) return token;

  std::string s = token;
  
  static const char* const suf_verb[] = {
    "ировавшись","ировались","ировалась","ировало","ировать","ируются","ируется",
    "авшись","явшись","ившись","ывшись","вшись",
    "ешь","ишь","ете","ите","ют","ут","ят",
    "аем","яем","ает","яет","аемся","яются",
    "ать","ять","ить","еть","уть","ти",
    "ал","ала","ало","али","ил","ила","ило","или"
  };

  static const char* const suf_adj[] = {
    "ейшего","ейшей","ейшие","ейший",
    "ого","его","ому","ему",
    "ыми","ими",
    "ая","яя","ое","ее","ые","ие",
    "ой","ей","ым","им","ом","ем",
    "ую","юю","ых","их"
  };

  static const char* const suf_noun[] = {
    "ирования","ирование","ированиям","ированиях",
    "ациями","ацией","ация","ации","ацию",
    "ениями","ением","ение","ения","ению",
    "остями","остью","ость","остей",
    "ами","ями","ах","ях",
    "ов","ев","ей",
    "ом","ем","ам","ям",
    "а","я","о","е","ы","и","у","ю","ь"
  };

  std::string before = s;
  if (remove_longest_suffix(s, suf_verb, (int)(sizeof(suf_verb)/sizeof(suf_verb[0])))) {
    if (utf8_len_chars(s) >= 3) return s;
    s = before;
  }

  before = s;
  if (remove_longest_suffix(s, suf_adj, (int)(sizeof(suf_adj)/sizeof(suf_adj[0])))) {
    if (utf8_len_chars(s) >= 3) return s;
    s = before;
  }

  before = s;
  if (remove_longest_suffix(s, suf_noun, (int)(sizeof(suf_noun)/sizeof(suf_noun[0])))) {
    if (utf8_len_chars(s) >= 3) return s;
    s = before;
  }

  return s;
}