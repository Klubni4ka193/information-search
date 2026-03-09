#pragma once
#include <string>
#include <vector>
#include <cstdint>

bool read_file_utf8(const std::string& path, std::string& out);

std::string trim(const std::string& s);
void split_by_char(const std::string& s, char delim, std::vector<std::string>& out);

bool str_starts_with(const std::string& s, const std::string& pfx);

void merge_sort_strings(std::vector<std::string>& a);
void merge_sort_pairs_term_doc(std::vector<std::string>& terms, std::vector<uint32_t>& docs);

struct TermDoc {
  std::string term;
  uint32_t doc;
};

void merge_sort_termdoc(std::vector<TermDoc>& a);

int bin_search_terms(const std::vector<std::string>& terms, const std::string& key);