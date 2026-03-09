\
#include "tokenizer.h"
#include "stemmer.h"
#include "util.h"

#include <cstdio>
#include <fstream>
#include <iostream>
#include <string>

static bool read_lines(const std::string& path, std::vector<std::string>& out) {
  out.clear();
  std::ifstream in(path);
  if (!in) return false;
  std::string line;
  while (std::getline(in, line)) {
    line = trim(line);
    if (!line.empty()) out.push_back(line);
  }
  return true;
}

static std::string run_path(const std::string& dir, int idx) {
  return dir + "/run_" + std::to_string(idx) + ".txt";
}

static bool write_run(const std::string& path, std::vector<std::string>& tokens) {
  merge_sort_strings(tokens);
  std::ofstream out(path, std::ios::binary);
  if (!out) return false;
  for (size_t i = 0; i < tokens.size(); ++i) {
    out.write(tokens[i].data(), (std::streamsize)tokens[i].size());
    out.put('\n');
  }
  return true;
}

static bool read_token_line(std::ifstream& in, std::string& tok) {
  tok.clear();
  return (bool)std::getline(in, tok);
}

int main(int argc, char** argv) {
  if (argc < 3) {
    std::cerr << "Usage: lab3_termfreq <docs_list.txt> <out_termfreq.tsv> [--stemming 0|1] [--chunk 2000000]\n";
    return 1;
  }
  std::string list_path = argv[1];
  std::string out_path = argv[2];

  bool use_stemming = false;
  int chunk = 2000000;
  for (int i = 3; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "--stemming" && i + 1 < argc) { use_stemming = (std::string(argv[i+1]) == "1"); i++; }
    else if (a == "--chunk" && i + 1 < argc) { chunk = std::stoi(argv[i+1]); i++; }
  }

  std::vector<std::string> files;
  if (!read_lines(list_path, files)) {
    std::cerr << "Cannot read list: " << list_path << "\n";
    return 2;
  }

  TokenizerConfig tc;
  Tokenizer tokenizer(tc);
  RussianStemmer stemmer;

  std::vector<std::string> buf;
  buf.reserve((size_t)chunk);


  std::string tmpdir = "tmp_termfreq";

#ifdef _WIN32
  std::string cmd = "mkdir " + tmpdir;
#else
  std::string cmd = "mkdir -p " + tmpdir;
#endif
  std::system(cmd.c_str());

  int run_count = 0;

  std::vector<std::string> tokens;
  for (size_t di = 0; di < files.size(); ++di) {
    std::string text;
    if (!read_file_utf8(files[di], text)) continue;

    tokenizer.tokenize(text, tokens);
    if (use_stemming) {
      for (size_t i = 0; i < tokens.size(); ++i) tokens[i] = stemmer.stem(tokens[i]);
    }

    for (size_t i = 0; i < tokens.size(); ++i) {
      buf.push_back(tokens[i]);
      if ((int)buf.size() >= chunk) {
        std::string rp = run_path(tmpdir, run_count++);
        if (!write_run(rp, buf)) {
          std::cerr << "Failed to write run: " << rp << "\n";
          return 3;
        }
        buf.clear();
      }
    }
  }

  if (!buf.empty()) {
    std::string rp = run_path(tmpdir, run_count++);
    if (!write_run(rp, buf)) {
      std::cerr << "Failed to write run: " << rp << "\n";
      return 3;
    }
    buf.clear();
  }

  if (run_count == 0) {
    std::ofstream(out_path) << "";
    return 0;
  }

  std::vector<std::ifstream*> ins;
  std::vector<std::string> cur;
  ins.reserve((size_t)run_count);
  cur.reserve((size_t)run_count);

  for (int i = 0; i < run_count; ++i) {
    auto* f = new std::ifstream(run_path(tmpdir, i), std::ios::binary);
    if (!(*f)) { std::cerr << "Cannot open run\n"; return 4; }
    ins.push_back(f);
    std::string tok;
    if (read_token_line(*f, tok)) cur.push_back(tok);
    else cur.push_back("");
  }

  std::vector<char> eof((size_t)run_count, 0);
  for (int i = 0; i < run_count; ++i) {
    if (!(*ins[(size_t)i])) eof[(size_t)i] = 1;
    if (cur[(size_t)i].empty() && ins[(size_t)i]->eof()) eof[(size_t)i] = 1;
  }

  std::ofstream out(out_path);
  if (!out) { std::cerr << "Cannot open output\n"; return 5; }

  auto all_done = [&]() {
    for (int i = 0; i < run_count; ++i) if (!eof[(size_t)i]) return false;
    return true;
  };

  std::string current_term;
  long long current_count = 0;

  while (!all_done()) {
    int best = -1;
    for (int i = 0; i < run_count; ++i) {
      if (eof[(size_t)i]) continue;
      if (best < 0 || cur[(size_t)i] < cur[(size_t)best]) best = i;
    }
    if (best < 0) break;

    std::string tok = cur[(size_t)best];

    std::string next;
    if (read_token_line(*ins[(size_t)best], next)) {
      cur[(size_t)best] = next;
    } else {
      eof[(size_t)best] = 1;
      cur[(size_t)best].clear();
    }

    if (current_term.empty()) {
      current_term = tok;
      current_count = 1;
    } else if (tok == current_term) {
      current_count++;
    } else {
      out << current_term << "\t" << current_count << "\n";
      current_term = tok;
      current_count = 1;
    }
  }

  if (!current_term.empty()) out << current_term << "\t" << current_count << "\n";

  for (int i = 0; i < run_count; ++i) {
    ins[(size_t)i]->close();
    delete ins[(size_t)i];
  }

  std::cerr << "runs=" << run_count << "\n";
  return 0;
}