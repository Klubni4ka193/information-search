#include "tokenizer.h"
#include "stemmer.h"
#include "util.h"
#include <vector>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <string>

struct LexEntry {
  std::string term;
  uint64_t off; 
  uint32_t df;
};

static uint16_t read_u16(std::ifstream& in) { uint16_t x; in.read((char*)&x, sizeof(x)); return x; }
static uint32_t read_u32(std::ifstream& in) { uint32_t x; in.read((char*)&x, sizeof(x)); return x; }
static uint64_t read_u64(std::ifstream& in) { uint64_t x; in.read((char*)&x, sizeof(x)); return x; }

static bool load_terms(const std::string& path, std::vector<LexEntry>& lex) {
  std::ifstream in(path, std::ios::binary);
  if (!in) return false;
  char magic[4];
  in.read(magic, 4);
  if (std::string(magic, 4) != "BIDX") return false;
  uint32_t ver = read_u32(in);
  if (ver != 1) return false;
  uint32_t n = read_u32(in);
  lex.clear();
  lex.reserve(n);
  for (uint32_t i = 0; i < n; ++i) {
    uint16_t len = read_u16(in);
    std::string term;
    term.resize(len);
    if (len) in.read(&term[0], len);
    uint64_t off = read_u64(in);
    uint32_t df = read_u32(in);
    LexEntry e{term, off, df};
    lex.push_back(e);
  }
  return true;
}

static bool load_docs(const std::string& path, std::vector<std::string>& urls, std::vector<std::string>& titles) {
  std::ifstream in(path, std::ios::binary);
  if (!in) return false;
  char magic[4];
  in.read(magic, 4);
  if (std::string(magic, 4) != "DOCS") return false;
  uint32_t ver = read_u32(in);
  if (ver != 1) return false;
  uint32_t n = read_u32(in);
  urls.resize(n);
  titles.resize(n);

  for (uint32_t i = 0; i < n; ++i) {
    uint16_t ulen = read_u16(in);
    std::string url; url.resize(ulen);
    if (ulen) in.read(&url[0], ulen);

    uint16_t tlen = read_u16(in);
    std::string title; title.resize(tlen);
    if (tlen) in.read(&title[0], tlen);

    urls[i] = url;
    titles[i] = title;
  }
  return true;
}

static int lex_find(const std::vector<LexEntry>& lex, const std::string& term) {
  int l = 0;
  int r = (int)lex.size() - 1;
  while (l <= r) {
    int m = l + (r - l) / 2;
    if (lex[(size_t)m].term == term) return m;
    if (lex[(size_t)m].term < term) l = m + 1;
    else r = m - 1;
  }
  return -1;
}

static void read_postings(std::ifstream& postings, const LexEntry& e, std::vector<uint32_t>& out) {
  out.clear();
  out.resize(e.df);
  postings.seekg((std::streamoff)e.off);
  if (e.df) postings.read((char*)&out[0], (std::streamsize)(e.df * sizeof(uint32_t)));
}

static void intersect(const std::vector<uint32_t>& a, const std::vector<uint32_t>& b, std::vector<uint32_t>& out) {
  out.clear();
  size_t i = 0, j = 0;
  while (i < a.size() && j < b.size()) {
    if (a[i] == b[j]) { out.push_back(a[i]); i++; j++; }
    else if (a[i] < b[j]) i++;
    else j++;
  }
}

static void uni(const std::vector<uint32_t>& a, const std::vector<uint32_t>& b, std::vector<uint32_t>& out) {
  out.clear();
  size_t i = 0, j = 0;
  while (i < a.size() && j < b.size()) {
    if (a[i] == b[j]) { out.push_back(a[i]); i++; j++; }
    else if (a[i] < b[j]) { out.push_back(a[i]); i++; }
    else { out.push_back(b[j]); j++; }
  }
  while (i < a.size()) out.push_back(a[i++]);
  while (j < b.size()) out.push_back(b[j++]);
}

static void diff(const std::vector<uint32_t>& a, const std::vector<uint32_t>& b, std::vector<uint32_t>& out) {
  out.clear();
  size_t i = 0, j = 0;
  while (i < a.size()) {
    if (j >= b.size()) { out.push_back(a[i++]); continue; }
    if (a[i] == b[j]) { i++; j++; }
    else if (a[i] < b[j]) { out.push_back(a[i]); i++; }
    else { j++; }
  }
}

static void complement_all(uint32_t doc_count, const std::vector<uint32_t>& a, std::vector<uint32_t>& out) {
  out.clear();
  size_t j = 0;
  for (uint32_t d = 0; d < doc_count; ++d) {
    while (j < a.size() && a[j] < d) j++;
    if (j < a.size() && a[j] == d) continue;
    out.push_back(d);
  }
}

enum TokType { TT_TERM, TT_AND, TT_OR, TT_NOT, TT_LP, TT_RP };

struct Tok {
  TokType t;
  std::string term;
};

static bool is_op_word(const std::string& s, const char* w) {
  if (s.size() != std::string(w).size()) return false;
  for (size_t i = 0; i < s.size(); ++i) {
    char c = s[i];
    char d = w[i];
    if (c >= 'A' && c <= 'Z') c = (char)(c - 'A' + 'a');
    if (d >= 'A' && d <= 'Z') d = (char)(d - 'A' + 'a');
    if (c != d) return false;
  }
  return true;
}

static int precedence(TokType t) {
  if (t == TT_NOT) return 3;
  if (t == TT_AND) return 2;
  if (t == TT_OR) return 1;
  return 0;
}

static bool is_unary(TokType t) { return t == TT_NOT; }

static bool is_operator_word(const std::string& s) {
    if (s.empty()) return false;
    
    std::string lower = s;
    for (char& c : lower) {
        if (c >= 'A' && c <= 'Z') c = c - 'A' + 'a';
    }
    
    return lower == "and" || lower == "or" || lower == "not";
}

static void tokenize_query(const std::string& q,
                           std::vector<Tok>& out,
                           const Tokenizer& tokenizer,
                           const RussianStemmer& stemmer,
                           bool use_stemming) {
    out.clear();
    std::string cur;
    bool last_was_term_or_rp = false; 
    
    auto flush_word = [&]() {
        if (cur.empty()) return;

        std::string lower_cur = cur;
        for (char& c : lower_cur) {
            if (c >= 'A' && c <= 'Z') c = c - 'A' + 'a';
        }
        
        if (lower_cur == "and" || lower_cur == "or" || lower_cur == "not") {
            if (lower_cur == "and") {
                out.push_back({TT_AND, "AND"});
                last_was_term_or_rp = false;
            }
            else if (lower_cur == "or") {
                out.push_back({TT_OR, "OR"});
                last_was_term_or_rp = false;
            }
            else if (lower_cur == "not") {
                if (last_was_term_or_rp) {
                    out.push_back({TT_AND, "AND"});
                }
                out.push_back({TT_NOT, "NOT"});
                last_was_term_or_rp = false;
            }
        } else {
            std::vector<std::string> ts;
            tokenizer.tokenize(cur, ts);

            for (auto &raw : ts) {
                if (raw.empty()) continue;
                
                std::string term = raw; 
                if (use_stemming) term = stemmer.stem(term);
                if (term.empty()) continue;
                
                if (last_was_term_or_rp) {
                    out.push_back({TT_AND, "AND"});
                }
                
                out.push_back({TT_TERM, term});
                last_was_term_or_rp = true;
            }
        }
        
        cur.clear();
    };

    for (char ch : q) {
        unsigned char c = (unsigned char)ch;

        if (c == '(') {
            flush_word();
            if (last_was_term_or_rp) {
                out.push_back({TT_AND, "AND"});
            }
            out.push_back({TT_LP, "("});
            last_was_term_or_rp = false;
        } else if (c == ')') {
            flush_word();
            out.push_back({TT_RP, ")"});
            last_was_term_or_rp = true; 
        } else if (c == '&') {
            flush_word();
            out.push_back({TT_AND, "AND"});
            last_was_term_or_rp = false;
        } else if (c == '|') {
            flush_word();
            out.push_back({TT_OR, "OR"});
            last_was_term_or_rp = false;
        } else if (c == '!') {
            flush_word();
            if (last_was_term_or_rp) {
                out.push_back({TT_AND, "AND"});
            }
            out.push_back({TT_NOT, "NOT"});
            last_was_term_or_rp = false;
        } else if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
            flush_word();
        } else {
            cur.push_back((char)c);
        }
    }
    flush_word();
}
static bool to_postfix(const std::vector<Tok>& in, std::vector<Tok>& out) {
  out.clear();
  std::vector<Tok> stack;
  
  for (const Tok& tok : in) {
    switch (tok.t) {
      case TT_TERM:
        out.push_back(tok);
        break;
        
      case TT_NOT:
        stack.push_back(tok);
        break;
        
      case TT_AND:
      case TT_OR:
        while (!stack.empty()) {
          Tok top = stack.back();
          if (top.t == TT_LP) break;
          if (precedence(top.t) >= precedence(tok.t)) {
            out.push_back(top);
            stack.pop_back();
          } else {
            break;
          }
        }
        stack.push_back(tok);
        break;
        
      case TT_LP:
        stack.push_back(tok);
        break;
        
      case TT_RP:
        while (!stack.empty() && stack.back().t != TT_LP) {
          out.push_back(stack.back());
          stack.pop_back();
        }
        if (stack.empty() || stack.back().t != TT_LP) {
          return false; 
        }
        stack.pop_back(); 
        
       
        if (!stack.empty() && stack.back().t == TT_NOT) {
          out.push_back(stack.back());
          stack.pop_back();
        }
        break;
        
      default:
        return false;
    }
  }
  
  while (!stack.empty()) {
    if (stack.back().t == TT_LP || stack.back().t == TT_RP) {
      return false; 
    }
    out.push_back(stack.back());
    stack.pop_back();
  }
  
  return true;
}


static bool eval_postfix(
  const std::vector<Tok>& pf,
  uint32_t doc_count,
  const std::vector<LexEntry>& lex,
  std::ifstream& postings,
  std::vector<uint32_t>& out
) {
  std::vector< std::vector<uint32_t> > st;
  st.reserve(16);

  std::vector<uint32_t> tmp1, tmp2, tmp3;

  for (size_t i = 0; i < pf.size(); ++i) {
    const Tok& t = pf[i];
    if (t.t == TT_TERM) {
      int idx = lex_find(lex, t.term);
      std::vector<uint32_t> v;
      if (idx >= 0) {
        read_postings(postings, lex[(size_t)idx], v);
      } else {
        v.clear();
      }
      st.push_back(v);
      continue;
    }

    if (t.t == TT_NOT) {
      if (st.empty()) {
        out.clear();
        return false;
      }
      tmp1 = st.back(); st.pop_back();
      complement_all(doc_count, tmp1, tmp2);
      st.push_back(tmp2);
      continue;
    }

    if (t.t == TT_AND || t.t == TT_OR) {
      if (st.size() < 2) {
        out.clear();
        return false;
      }
      tmp2 = st.back(); st.pop_back();
      tmp1 = st.back(); st.pop_back();
      if (t.t == TT_AND) {
        intersect(tmp1, tmp2, tmp3);
      } else {
        uni(tmp1, tmp2, tmp3);
      }
      st.push_back(tmp3);
      continue;
    }
    return false;
  }

  if (st.size() != 1) {
    out.clear();
    return false;
  }
  out = st.back();
  return true;
}

int main(int argc, char** argv) {
  if (argc < 3) {
    std::cerr << "Usage: bool_search_cli <index_dir> <query> [--limit N] [--stemming 0|1]\n";
    return 1;
  }
  std::string index_dir = argv[1];
  std::string query = argv[2];

  int limit = 20;
  bool use_stemming = true;
  for (int i = 3; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "--limit" && i + 1 < argc) { limit = std::stoi(argv[i+1]); i++; }
    else if (a == "--stemming" && i + 1 < argc) { use_stemming = (std::string(argv[i+1]) == "1"); i++; }
  }

  std::vector<LexEntry> lex;
  if (!load_terms(index_dir + "/terms.bin", lex)) {
    std::cerr << "Cannot load terms.bin\n";
    return 2;
  }

  std::vector<std::string> urls, titles;
  if (!load_docs(index_dir + "/docs.bin", urls, titles)) {
    std::cerr << "Cannot load docs.bin\n";
    return 3;
  }
  uint32_t doc_count = (uint32_t)urls.size();

  std::ifstream postings(index_dir + "/postings.bin", std::ios::binary);
  if (!postings) {
    std::cerr << "Cannot open postings.bin\n";
    return 4;
  }

  TokenizerConfig tc;
  tc.lowercase = true;
  tc.normalize_yo = true;
  Tokenizer tokenizer(tc);

  RussianStemmer stemmer;

  std::vector<Tok> toks;
  tokenize_query(query, toks, tokenizer, stemmer, use_stemming);

  std::cerr << "Tokens after tokenize_query: ";
  for (const auto& t : toks) {
    if (t.t == TT_TERM) std::cerr << "TERM:" << t.term << " ";
    else if (t.t == TT_AND) std::cerr << "AND ";
    else if (t.t == TT_OR) std::cerr << "OR ";
    else if (t.t == TT_NOT) std::cerr << "NOT ";
    else if (t.t == TT_LP) std::cerr << "( ";
    else if (t.t == TT_RP) std::cerr << ") ";
  }
  std::cerr << "\n";

  std::vector<Tok> pf;
  if (!to_postfix(toks, pf)) {
    std::cerr << "Parse error\n";
    return 5;
  }

  std::cerr << "Postfix tokens: ";
  for (const auto& t : pf) {
    if (t.t == TT_TERM) std::cerr << "[TERM:" << t.term << "] ";
    else if (t.t == TT_AND) std::cerr << "[AND] ";
    else if (t.t == TT_OR) std::cerr << "[OR] ";
    else if (t.t == TT_NOT) std::cerr << "[NOT] ";
  }
  std::cerr << "\n";

  std::vector<uint32_t> res;
  if (!eval_postfix(pf, doc_count, lex, postings, res)) {
    std::cerr << "Eval error\n";
    return 6;
  }

  int shown = 0;
  for (size_t i = 0; i < res.size() && shown < limit; ++i) {
    uint32_t d = res[i];
    if (d >= doc_count) continue;
    std::string url = urls[d];
    std::string title = titles[d];
    if (title.empty()) title = url;
    std::cout << d << "\t" << url << "\t" << title << "\n";
    shown++;
  }
  return 0;
}