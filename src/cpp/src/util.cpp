#include "util.h"
#include <fstream>
#include <iostream>

bool read_file_utf8(const std::string& path, std::string& out) {
  std::ifstream in(path, std::ios::binary);
  if (!in) return false;
  in.seekg(0, std::ios::end);
  std::streampos sz = in.tellg();
  if (sz < 0) sz = 0;
  in.seekg(0, std::ios::beg);
  out.resize((size_t)sz);
  if (sz > 0) in.read(&out[0], sz);
  return true;
}

std::string trim(const std::string& s) {
  size_t i = 0, j = s.size();
  while (i < j && (s[i] == ' ' || s[i] == '\t' || s[i] == '\r' || s[i] == '\n')) i++;
  while (j > i && (s[j-1] == ' ' || s[j-1] == '\t' || s[j-1] == '\r' || s[j-1] == '\n')) j--;
  return s.substr(i, j - i);
}

void split_by_char(const std::string& s, char delim, std::vector<std::string>& out) {
  out.clear();
  std::string cur;
  for (size_t i = 0; i < s.size(); ++i) {
    if (s[i] == delim) {
      out.push_back(cur);
      cur.clear();
    } else {
      cur.push_back(s[i]);
    }
  }
  out.push_back(cur);
}

bool str_starts_with(const std::string& s, const std::string& pfx) {
  if (s.size() < pfx.size()) return false;
  return s.compare(0, pfx.size(), pfx) == 0;
}

static void merge_sort_strings_rec(std::vector<std::string>& a, std::vector<std::string>& tmp, int l, int r) {
  if (r - l <= 1) return;
  int m = (l + r) / 2;
  merge_sort_strings_rec(a, tmp, l, m);
  merge_sort_strings_rec(a, tmp, m, r);
  int i = l, j = m, k = l;
  while (i < m && j < r) {
    if (a[i] <= a[j]) tmp[k++] = a[i++];
    else tmp[k++] = a[j++];
  }
  while (i < m) tmp[k++] = a[i++];
  while (j < r) tmp[k++] = a[j++];
  for (int p = l; p < r; ++p) a[p] = tmp[p];
}

void merge_sort_strings(std::vector<std::string>& a) {
  std::vector<std::string> tmp(a.size());
  merge_sort_strings_rec(a, tmp, 0, (int)a.size());
}

static void merge_sort_pairs_rec(std::vector<std::string>& terms, std::vector<uint32_t>& docs,
                                std::vector<std::string>& t2, std::vector<uint32_t>& d2,
                                int l, int r) {
  if (r - l <= 1) return;
  int m = (l + r) / 2;
  merge_sort_pairs_rec(terms, docs, t2, d2, l, m);
  merge_sort_pairs_rec(terms, docs, t2, d2, m, r);

  int i = l, j = m, k = l;
  while (i < m && j < r) {
    bool le = (terms[i] < terms[j]) || (terms[i] == terms[j] && docs[i] <= docs[j]);
    if (le) { t2[k] = terms[i]; d2[k] = docs[i]; i++; }
    else { t2[k] = terms[j]; d2[k] = docs[j]; j++; }
    k++;
  }
  while (i < m) { t2[k] = terms[i]; d2[k] = docs[i]; i++; k++; }
  while (j < r) { t2[k] = terms[j]; d2[k] = docs[j]; j++; k++; }

  for (int p = l; p < r; ++p) { terms[p] = t2[p]; docs[p] = d2[p]; }
}

void merge_sort_pairs_term_doc(std::vector<std::string>& terms, std::vector<uint32_t>& docs) {
  std::vector<std::string> t2(terms.size());
  std::vector<uint32_t> d2(docs.size());
  merge_sort_pairs_rec(terms, docs, t2, d2, 0, (int)terms.size());
}

static void merge_sort_td_rec(std::vector<TermDoc>& a, std::vector<TermDoc>& tmp, int l, int r) {
  if (r - l <= 1) return;
  int m = (l + r) / 2;
  merge_sort_td_rec(a, tmp, l, m);
  merge_sort_td_rec(a, tmp, m, r);
  int i = l, j = m, k = l;
  while (i < m && j < r) {
    bool le = (a[i].term < a[j].term) || (a[i].term == a[j].term && a[i].doc <= a[j].doc);
    if (le) tmp[k++] = a[i++];
    else tmp[k++] = a[j++];
  }
  while (i < m) tmp[k++] = a[i++];
  while (j < r) tmp[k++] = a[j++];
  for (int p = l; p < r; ++p) a[p] = tmp[p];
}

void merge_sort_termdoc(std::vector<TermDoc>& a) {
  std::vector<TermDoc> tmp(a.size());
  merge_sort_td_rec(a, tmp, 0, (int)a.size());
}

int bin_search_terms(const std::vector<std::string>& terms, const std::string& key) {
  int l = 0;
  int r = (int)terms.size() - 1;
  while (l <= r) {
    int m = l + (r - l) / 2;
    if (terms[m] == key) return m;
    if (terms[m] < key) l = m + 1;
    else r = m - 1;
  }
  return -1;
}