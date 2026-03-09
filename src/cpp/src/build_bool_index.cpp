#include "tokenizer.h"
#include "stemmer.h"
#include "util.h"

#include <cstdint>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

static void write_u16(std::ofstream& out, uint16_t x) { out.write((char*)&x, sizeof(x)); }
static void write_u32(std::ofstream& out, uint32_t x) { out.write((char*)&x, sizeof(x)); }
static void write_u64(std::ofstream& out, uint64_t x) { out.write((char*)&x, sizeof(x)); }

static uint16_t read_u16(std::ifstream& in) { uint16_t x; in.read((char*)&x, sizeof(x)); return x; }
static uint32_t read_u32(std::ifstream& in) { uint32_t x; in.read((char*)&x, sizeof(x)); return x; }

static std::string sanitize_field(std::string s) {
  for (char& c : s) {
    if (c == '\t' || c == '\n' || c == '\r') c = ' ';
  }
  return s;
}

struct Args {
  std::string docs_list_abs;
  std::string meta_docid_tsv;
  std::string out_dir;
  bool stemming = true;
  uint64_t chunk_pairs = 2000000;
};

static bool parse_args(int argc, char** argv, Args& a) {
  if (argc < 4) return false;
  a.docs_list_abs = argv[1];
  a.meta_docid_tsv = argv[2];
  a.out_dir = argv[3];

  for (int i = 4; i < argc; ++i) {
    std::string s = argv[i];
    if (s == "--stemming" && i + 1 < argc) {
      a.stemming = (std::string(argv[i + 1]) == "1");
      i++;
    } else if (s == "--chunk_pairs" && i + 1 < argc) {
      a.chunk_pairs = (uint64_t)std::stoull(argv[i + 1]);
      i++;
    }
  }
  return true;
}

static bool read_lines(const std::string& path, std::vector<std::string>& out) {
  std::ifstream in(path);
  if (!in) return false;
  out.clear();
  std::string line;
  while (std::getline(in, line)) {
    line = trim(line);
    if (!line.empty()) out.push_back(line);
  }
  return true;
}

static bool split_tsv6_fast(const std::string& line, std::string parts[6]) {
  size_t pos = 0;
  for (int i = 0; i < 5; ++i) {
    size_t tab = line.find('\t', pos);
    if (tab == std::string::npos) return false;
    parts[i] = line.substr(pos, tab - pos);
    pos = tab + 1;
  }
  parts[5] = line.substr(pos);
  return true;
}

static bool build_docs_bin(const std::string& meta_docid_tsv,
                           uint32_t doc_count,
                           const std::string& out_docs_bin) {
  std::vector<std::string> urls(doc_count), titles(doc_count);

  std::ifstream in(meta_docid_tsv);
  if (!in) {
    std::cerr << "Cannot open meta_docid.tsv: " << meta_docid_tsv << "\n";
    return false;
  }

  std::string header;
  if (!std::getline(in, header)) {
    std::cerr << "meta_docid.tsv empty\n";
    return false;
  }

  std::string line;
  while (std::getline(in, line)) {
    if (line.empty()) continue;
    std::string p[6];
    if (!split_tsv6_fast(line, p)) continue;

    int id = -1;
    try { id = std::stoi(p[0]); } catch (...) { continue; }
    if (id < 0 || (uint32_t)id >= doc_count) continue;

    urls[(size_t)id] = sanitize_field(p[1]);
    titles[(size_t)id] = sanitize_field(p[4]);
  }

  std::ofstream out(out_docs_bin, std::ios::binary);
  if (!out) {
    std::cerr << "Cannot write docs.bin: " << out_docs_bin << "\n";
    return false;
  }

  out.write("DOCS", 4);
  write_u32(out, 1);                
  write_u32(out, doc_count);

  for (uint32_t i = 0; i < doc_count; ++i) {
    const std::string& u = urls[(size_t)i];
    const std::string& t = titles[(size_t)i];

    uint16_t ulen = (uint16_t)((u.size() > 65535) ? 65535 : u.size());
    uint16_t tlen = (uint16_t)((t.size() > 65535) ? 65535 : t.size());

    write_u16(out, ulen);
    if (ulen) out.write(u.data(), ulen);

    write_u16(out, tlen);
    if (tlen) out.write(t.data(), tlen);
  }

  return true;
}

static bool write_run(const std::string& path, std::vector<TermDoc>& chunk) {
  merge_sort_termdoc(chunk);

  if (!chunk.empty()) {
    size_t w = 1;
    for (size_t i = 1; i < chunk.size(); ++i) {
      if (chunk[i].term == chunk[w - 1].term && chunk[i].doc == chunk[w - 1].doc) continue;
      chunk[w++] = chunk[i];
    }
    chunk.resize(w);
  }

  std::ofstream out(path, std::ios::binary);
  if (!out) return false;

  for (const auto& td : chunk) {
    uint16_t len = (uint16_t)((td.term.size() > 65535) ? 65535 : td.term.size());
    write_u16(out, len);
    if (len) out.write(td.term.data(), len);
    write_u32(out, td.doc);
  }
  return true;
}

struct RunReader {
  std::ifstream in;
  bool has = false;
  std::string term;
  uint32_t doc = 0;

  bool open(const std::string& path) {
    in.open(path, std::ios::binary);
    has = false;
    term.clear();
    doc = 0;
    return (bool)in;
  }

  bool next() {
    if (!in) { has = false; return false; }
    if (in.peek() == EOF) { has = false; return false; }
    uint16_t len;
    in.read((char*)&len, sizeof(len));
    if (!in) { has = false; return false; }
    term.resize(len);
    if (len) in.read(&term[0], len);
    if (!in) { has = false; return false; }
    doc = read_u32(in);
    if (!in) { has = false; return false; }
    has = true;
    return true;
  }
};

struct LexEntryOut {
  std::string term;
  uint64_t off;
  uint32_t df;
};

static bool build_terms_postings_from_runs(const std::vector<std::string>& run_paths,
                                          const std::string& out_terms_bin,
                                          const std::string& out_postings_bin) {
  std::vector<RunReader> runs(run_paths.size());
  for (size_t i = 0; i < run_paths.size(); ++i) {
    if (!runs[i].open(run_paths[i])) {
      std::cerr << "Cannot open run: " << run_paths[i] << "\n";
      return false;
    }
    runs[i].next(); 
  }

  std::ofstream postings(out_postings_bin, std::ios::binary);
  if (!postings) {
    std::cerr << "Cannot write postings.bin: " << out_postings_bin << "\n";
    return false;
  }

  std::vector<LexEntryOut> lex;
  lex.reserve(1024);

  std::string cur_term;
  std::vector<uint32_t> cur_post;
  cur_post.reserve(64);

  uint64_t postings_off = 0;

  std::string last_term;
  uint32_t last_doc = 0;
  bool have_last = false;

  auto flush_term = [&]() {
    if (cur_term.empty()) return;
    if (!cur_post.empty()) {
      postings.write((char*)&cur_post[0], (std::streamsize)(cur_post.size() * sizeof(uint32_t)));
      LexEntryOut e;
      e.term = cur_term;
      e.off = postings_off;
      e.df = (uint32_t)cur_post.size();
      lex.push_back(e);
      postings_off += (uint64_t)cur_post.size() * sizeof(uint32_t);
    }
    cur_term.clear();
    cur_post.clear();
  };

  while (true) {
    int best = -1;
    for (size_t i = 0; i < runs.size(); ++i) {
      if (!runs[i].has) continue;
      if (best < 0) { best = (int)i; continue; }
      const auto& a = runs[i];
      const auto& b = runs[(size_t)best];
      if (a.term < b.term || (a.term == b.term && a.doc < b.doc)) best = (int)i;
    }
    if (best < 0) break;

    std::string term = runs[(size_t)best].term;
    uint32_t doc = runs[(size_t)best].doc;

    runs[(size_t)best].next();

    if (have_last && term == last_term && doc == last_doc) continue;
    have_last = true; last_term = term; last_doc = doc;

    if (cur_term.empty()) {
      cur_term = term;
      cur_post.push_back(doc);
    } else if (term == cur_term) {
      if (cur_post.empty() || cur_post.back() != doc) cur_post.push_back(doc);
    } else {
      flush_term();
      cur_term = term;
      cur_post.push_back(doc);
    }
  }

  flush_term();
  postings.close();

  std::ofstream terms(out_terms_bin, std::ios::binary);
  if (!terms) {
    std::cerr << "Cannot write terms.bin: " << out_terms_bin << "\n";
    return false;
  }

  terms.write("BIDX", 4);
  write_u32(terms, 1);  
  write_u32(terms, (uint32_t)lex.size());

  for (const auto& e : lex) {
    uint16_t len = (uint16_t)((e.term.size() > 65535) ? 65535 : e.term.size());
    write_u16(terms, len);
    if (len) terms.write(e.term.data(), len);
    write_u64(terms, e.off);
    write_u32(terms, e.df);
  }

  return true;
}

int main(int argc, char** argv) {
  Args a;
  if (!parse_args(argc, argv, a)) {
    std::cerr << "Usage: build_bool_index <docs_list_abs.txt> <meta_docid.tsv> <out_dir> "
                 "[--stemming 0|1] [--chunk_pairs N]\n";
    return 1;
  }

  {
    std::string cmd = "mkdir -p \"" + a.out_dir + "\"";
    (void)std::system(cmd.c_str());
  }

  std::vector<std::string> doc_paths;
  if (!read_lines(a.docs_list_abs, doc_paths)) {
    std::cerr << "Cannot read docs_list_abs: " << a.docs_list_abs << "\n";
    return 2;
  }
  uint32_t doc_count = (uint32_t)doc_paths.size();
  if (doc_count == 0) {
    std::cerr << "docs_list_abs is empty\n";
    return 3;
  }

  {
    std::string out_docs_bin = a.out_dir + "/docs.bin";
    if (!build_docs_bin(a.meta_docid_tsv, doc_count, out_docs_bin)) {
      std::cerr << "Failed to build docs.bin\n";
      return 4;
    }
  }

  TokenizerConfig tc;
  tc.lowercase = true;
  tc.normalize_yo = true;
  Tokenizer tokenizer(tc);
  RussianStemmer stemmer;

  std::vector<TermDoc> chunk;
  chunk.reserve((size_t)((a.chunk_pairs > 5000000ULL) ? 5000000ULL : a.chunk_pairs));

  std::vector<std::string> run_paths;
  run_paths.reserve(64);

  auto flush_chunk_to_run = [&](int run_id) -> bool {
    if (chunk.empty()) return true;
    std::string run_path = a.out_dir + "/run_" + std::to_string(run_id) + ".bin";
    if (!write_run(run_path, chunk)) {
      std::cerr << "Failed to write run: " << run_path << "\n";
      return false;
    }
    run_paths.push_back(run_path);
    chunk.clear();
    return true;
  };

  int run_id = 0;

  for (uint32_t doc_id = 0; doc_id < doc_count; ++doc_id) {
    std::string text;
    if (!read_file_utf8(doc_paths[(size_t)doc_id], text)) {
      continue;
    }

    std::vector<std::string> toks;
    tokenizer.tokenize(text, toks);

    if (a.stemming) {
      for (auto& t : toks) t = stemmer.stem(t);
    }

    merge_sort_strings(toks);
    if (!toks.empty()) {
      size_t w = 1;
      for (size_t i = 1; i < toks.size(); ++i) {
        if (toks[i] == toks[w - 1]) continue;
        toks[w++] = toks[i];
      }
      toks.resize(w);
    }

    for (const auto& t : toks) {
      if (t.empty()) continue;
      TermDoc td;
      td.term = t;
      td.doc = doc_id;
      chunk.push_back(td);

      if ((uint64_t)chunk.size() >= a.chunk_pairs) {
        if (!flush_chunk_to_run(run_id++)) return 5;
      }
    }

    if ((doc_id + 1) % 5000 == 0) {
      std::cerr << "[index] processed=" << (doc_id + 1) << "/" << doc_count
                << " runs=" << run_paths.size() << "\n";
    }
  }

  if (!flush_chunk_to_run(run_id++)) return 6;
  if (run_paths.empty()) {
    std::cerr << "No runs created (no tokens?)\n";
    return 7;
  }

  std::string out_terms = a.out_dir + "/terms.bin";
  std::string out_postings = a.out_dir + "/postings.bin";

  if (!build_terms_postings_from_runs(run_paths, out_terms, out_postings)) {
    std::cerr << "Failed to build terms/postings\n";
    return 8;
  }

  for (const auto& rp : run_paths) {
    std::remove(rp.c_str());
  }

  return 0;
}