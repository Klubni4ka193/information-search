SHELL := /bin/bash
.ONESHELL:
.SHELLFLAGS := -euo pipefail -c
.DEFAULT_GOAL := help

ROOT := $(CURDIR)
OUT_DIR ?= ./corpus_out
DOCS_DIR := $(OUT_DIR)/docs
CPP_DIR := cpp/src
BIN_DIR := $(CPP_DIR)/bin
SCRIPTS_DIR := scripts

CFG ?= $(firstword $(wildcard crawler/config.yaml config.yaml))

CRAWLER  := $(firstword $(wildcard crawler/crawler.py crawler.py))
EXPORTER := $(firstword $(wildcard crawler/export_corpus.py export_corpus.py))
MONITOR  := $(firstword $(wildcard crawler/monitor.py monitor.py))
REQS     := $(firstword $(wildcard crawler/requirements.txt requirements.txt))

STEMMING ?= 1      
CHUNK ?= 2000000
CHUNK_PAIRS ?= 2000000
LIMIT ?= 10

DOCS_LIST := $(OUT_DIR)/docs_list.txt
DOCS_LIST_ABS := $(OUT_DIR)/docs_list_abs.txt
META_TSV := $(OUT_DIR)/meta.tsv
META_DOCID := $(OUT_DIR)/meta_docid.tsv

TOKENIZE_MARK := $(OUT_DIR)/.tokenize_ready
ACTIVE_STEM_FILE := $(OUT_DIR)/.active_stemming

VENV_PY := .venv/bin/python
VENV_PIP := .venv/bin/pip
DEPS_MARK := .venv/.deps_installed

LAB3_STATS_BIN := $(BIN_DIR)/lab3_tokenize_stats
LAB3_TERMFREQ_BIN := $(BIN_DIR)/lab3_termfreq
BUILD_BOOL_INDEX_BIN := $(BIN_DIR)/build_bool_index
BOOL_SEARCH_BIN := $(BIN_DIR)/bool_search_cli

.PHONY: help install deps download monitor tokenize zipf index search full \
        build_cpp require_tokenize check_scripts \
        termfreq zipf_plot bool_index bool_query \
        clean clean_index

help:
	@echo "Команды:"
	@echo "  make install                  - зависимости"
	@echo "  make download CFG=...         - скачать документы роботом"
	@echo "  make monitor  CFG=...         - мониторинг"
	@echo "  make tokenize STEMMING=0|1    - токенизация, с выбором включать ли стемминг"
	@echo "  make zipf                     - частоты + закон Ципфа"
	@echo "  make index                    - булев индекс"
	@echo "  make search Q='...'           - булев поиск"
	@echo "  make full  CFG=... STEMMING=0|1 - полный запуск"
	@echo ""
	@echo "Текущий активный режим стемминга хранится в: $(ACTIVE_STEM_FILE)"
	@echo "OUT_DIR is $(OUT_DIR)"


install: deps

deps: $(DEPS_MARK)

.venv/bin/python:
	python3 -m venv .venv

$(DEPS_MARK): .venv/bin/python
	@if [ -z "$(REQS)" ]; then echo "ERROR: requirements.txt не найден" && exit 2; fi
	$(VENV_PIP) install -r "$(REQS)"
	$(VENV_PIP) install -q matplotlib
	@touch "$(DEPS_MARK)"
	@echo "OK: deps installed"

download: deps
	@if [ -z "$(CRAWLER)" ]; then echo "ERROR: crawler.py не найден" && exit 2; fi
	@CFG_PATH="$(CFG)"; \
	if [ -d "$$CFG_PATH" ]; then CFG_PATH="$$CFG_PATH/config.yaml"; fi; \
	if [ ! -f "$$CFG_PATH" ]; then echo "ERROR: config file not found: $$CFG_PATH" && exit 2; fi; \
	$(VENV_PY) "$(CRAWLER)" "$$CFG_PATH"

monitor: deps
	@if [ -z "$(MONITOR)" ]; then echo "ERROR: monitor.py не найден" && exit 2; fi
	@CFG_PATH="$(CFG)"; \
	if [ -d "$$CFG_PATH" ]; then CFG_PATH="$$CFG_PATH/config.yaml"; fi; \
	if [ ! -f "$$CFG_PATH" ]; then echo "ERROR: config file not found: $$CFG_PATH" && exit 2; fi; \
	$(VENV_PY) "$(MONITOR)" "$$CFG_PATH"

check_scripts:
	@test -f "$(SCRIPTS_DIR)/make_docs_lists.py" || (echo "ERROR: missing $(SCRIPTS_DIR)/make_docs_lists.py" && exit 2)
	@test -f "$(SCRIPTS_DIR)/make_meta_docid.py" || (echo "ERROR: missing $(SCRIPTS_DIR)/make_meta_docid.py" && exit 2)
	@test -f "$(SCRIPTS_DIR)/zipf_plot.py" || (echo "ERROR: missing $(SCRIPTS_DIR)/zipf_plot.py" && exit 2)
	@echo "OK: scripts exist"


tokenize: deps check_scripts build_cpp
	@if [ -z "$(EXPORTER)" ]; then echo "ERROR: export_corpus.py не найден" && exit 2; fi
	@CFG_PATH="$(CFG)"; \
	if [ -d "$$CFG_PATH" ]; then CFG_PATH="$$CFG_PATH/config.yaml"; fi; \
	if [ ! -f "$$CFG_PATH" ]; then echo "ERROR: config file not found: $$CFG_PATH" && exit 2; fi; \
	mkdir -p "$(OUT_DIR)"
	@echo "[1/4] export -> $(OUT_DIR)"
	$(VENV_PY) "$(EXPORTER)" "$$CFG_PATH"
	@test -f "$(META_TSV)" || (echo "ERROR: $(META_TSV) не создан" && exit 2)
	@test -d "$(DOCS_DIR)" || (echo "ERROR: $(DOCS_DIR) не создан" && exit 2)

	@echo "[2/4] docs_list + docs_list_abs"
	$(VENV_PY) "$(SCRIPTS_DIR)/make_docs_lists.py" \
	  --docs_dir "$(DOCS_DIR)" \
	  --out_list "$(DOCS_LIST)" \
	  --out_abs "$(DOCS_LIST_ABS)"

	@echo "[3/4] meta_docid.tsv (fix stoi)"
	$(VENV_PY) "$(SCRIPTS_DIR)/make_meta_docid.py" \
	  --meta_in "$(META_TSV)" \
	  --docs_list_abs "$(DOCS_LIST_ABS)" \
	  --meta_out "$(META_DOCID)"

	@echo "[4/4] tokenization stats (stemming=$(STEMMING))"
	"$(LAB3_STATS_BIN)" "$(DOCS_LIST_ABS)" --stemming "$(STEMMING)" > "$(OUT_DIR)/lab3_tokenize_stats_s$(STEMMING).txt"
	@test -f "$(OUT_DIR)/lab3_tokenize_stats_s$(STEMMING).txt" || (echo "ERROR: $(OUT_DIR)/lab3_tokenize_stats_s$(STEMMING).txt не создан" && exit 2)

	@echo "$(STEMMING)" > "$(ACTIVE_STEM_FILE)"
	@touch "$(TOKENIZE_MARK)"
	@echo "OK: tokenize done. active stemming=$(STEMMING)"
	@echo "Marker: $(TOKENIZE_MARK)"


require_tokenize:
	@if [ ! -f "$(TOKENIZE_MARK)" ]; then \
	  echo "ERROR: tokenize ещё не выполнен"; \
	  echo "Сначала запусти: make tokenize STEMMING=0|1"; \
	  exit 2; \
	fi
	@test -f "$(ACTIVE_STEM_FILE)" || (echo "ERROR: нет $(ACTIVE_STEM_FILE), сделай make tokenize" && exit 2)
	@test -f "$(DOCS_LIST_ABS)" || (echo "ERROR: нет $(DOCS_LIST_ABS), сделай make tokenize" && exit 2)
	@test -f "$(META_DOCID)" || (echo "ERROR: нет $(META_DOCID), сделай make tokenize" && exit 2)


zipf: require_tokenize termfreq zipf_plot
	@S=$$(cat "$(ACTIVE_STEM_FILE)"); \
	echo "OK: zipf готово: $(OUT_DIR)/zipf_s$$S.png (csv: $(OUT_DIR)/zipf_s$$S.csv)"

termfreq: require_tokenize build_cpp
	@S=$$(cat "$(ACTIVE_STEM_FILE)"); \
	OUT="$(OUT_DIR)/termfreq_s$$S.tsv"; LOG="$(OUT_DIR)/termfreq_s$$S.log"; \
	"$(LAB3_TERMFREQ_BIN)" "$(DOCS_LIST_ABS)" "$$OUT" --stemming "$$S" --chunk "$(CHUNK)" 2> "$$LOG"; \
	echo "OK: wrote $$OUT"; \
	echo "Top-10 terms:"; \
	sort -t $$'\t' -k2,2nr "$$OUT" | head -n 10 || true

zipf_plot: require_tokenize
	@S=$$(cat "$(ACTIVE_STEM_FILE)"); \
	TF="$(OUT_DIR)/termfreq_s$$S.tsv"; \
	CSV="$(OUT_DIR)/zipf_s$$S.csv"; \
	PNG="$(OUT_DIR)/zipf_s$$S.png"; \
	test -f "$$TF" || (echo "ERROR: нет $$TF. Сначала: make zipf" && exit 2); \
	$(VENV_PY) "$(SCRIPTS_DIR)/zipf_plot.py" --termfreq "$$TF" --out_csv "$$CSV" --out_png "$$PNG"

index: require_tokenize bool_index
	@S=$$(cat "$(ACTIVE_STEM_FILE)"); \
	echo "OK: index готово: $(OUT_DIR)/index_bool_s$$S"

bool_index: require_tokenize build_cpp
	@S=$$(cat "$(ACTIVE_STEM_FILE)"); \
	DIR="$(OUT_DIR)/index_bool_s$$S"; \
	mkdir -p "$$DIR"; \
	"$(BUILD_BOOL_INDEX_BIN)" "$(DOCS_LIST_ABS)" "$(META_DOCID)" "$$DIR" --stemming "$$S" --chunk_pairs "$(CHUNK_PAIRS)"

search: require_tokenize bool_query

bool_query: require_tokenize build_cpp
	@S=$$(cat "$(ACTIVE_STEM_FILE)"); \
	DIR="$(OUT_DIR)/index_bool_s$$S"; \
	if [ ! -f "$$DIR/terms.bin" ]; then \
	  echo "ERROR: индекс не найден: $$DIR/terms.bin"; \
	  echo "Сначала запусти: make index"; \
	  exit 2; \
	fi; \
	if [ -z "$(strip $(Q))" ]; then \
	  echo "Не задан запрос. Пример:"; \
	  echo "  make search Q='transformer & attention'"; \
	  echo "  make search Q='(bert | transformer) & !survey'"; \
	  exit 2; \
	fi; \
	set +H; \
	"$(BOOL_SEARCH_BIN)" "$$DIR" "$(subst !,\\!,$(Q))" --limit "$(LIMIT)" --stemming "$$S"


full: deps download tokenize zipf index
	@echo "OK: full pipeline done"

build_cpp: $(LAB3_STATS_BIN) $(LAB3_TERMFREQ_BIN) $(BUILD_BOOL_INDEX_BIN) $(BOOL_SEARCH_BIN)

$(BIN_DIR):
	mkdir -p "$(BIN_DIR)"

$(LAB3_STATS_BIN): $(BIN_DIR) $(CPP_DIR)/lab3_tokenize_stats.cpp $(CPP_DIR)/tokenizer.cpp $(CPP_DIR)/stemmer.cpp $(CPP_DIR)/util.cpp
	g++ -O2 -std=c++17 -o "$@" $(CPP_DIR)/lab3_tokenize_stats.cpp $(CPP_DIR)/tokenizer.cpp $(CPP_DIR)/stemmer.cpp $(CPP_DIR)/util.cpp

$(LAB3_TERMFREQ_BIN): $(BIN_DIR) $(CPP_DIR)/lab3_termfreq.cpp $(CPP_DIR)/tokenizer.cpp $(CPP_DIR)/stemmer.cpp $(CPP_DIR)/util.cpp
	g++ -O2 -std=c++17 -o "$@" $(CPP_DIR)/lab3_termfreq.cpp $(CPP_DIR)/tokenizer.cpp $(CPP_DIR)/stemmer.cpp $(CPP_DIR)/util.cpp

$(BUILD_BOOL_INDEX_BIN): $(BIN_DIR) $(CPP_DIR)/build_bool_index.cpp $(CPP_DIR)/tokenizer.cpp $(CPP_DIR)/stemmer.cpp $(CPP_DIR)/util.cpp
	g++ -O2 -std=c++17 -o "$@" $(CPP_DIR)/build_bool_index.cpp $(CPP_DIR)/tokenizer.cpp $(CPP_DIR)/stemmer.cpp $(CPP_DIR)/util.cpp

$(BOOL_SEARCH_BIN): $(BIN_DIR) $(CPP_DIR)/bool_search_cli.cpp $(CPP_DIR)/tokenizer.cpp $(CPP_DIR)/stemmer.cpp $(CPP_DIR)/util.cpp
	g++ -O2 -std=c++17 -o "$@" $(CPP_DIR)/bool_search_cli.cpp $(CPP_DIR)/tokenizer.cpp $(CPP_DIR)/stemmer.cpp $(CPP_DIR)/util.cpp

clean:
	rm -rf "$(BIN_DIR)" .venv

clean_index:
	@S=$$(test -f "$(ACTIVE_STEM_FILE)" && cat "$(ACTIVE_STEM_FILE)" || echo ""); \
	rm -f "$(TOKENIZE_MARK)" "$(ACTIVE_STEM_FILE)"; \
	rm -f "$(DOCS_LIST)" "$(DOCS_LIST_ABS)" "$(META_DOCID)"; \
	rm -f "$(OUT_DIR)"/lab3_tokenize_stats_s*.txt; \
	rm -f "$(OUT_DIR)"/termfreq_s*.tsv "$(OUT_DIR)"/termfreq_s*.log; \
	rm -f "$(OUT_DIR)"/zipf_s*.csv "$(OUT_DIR)"/zipf_s*.png; \
	rm -rf "$(OUT_DIR)"/index_bool_s*; \
	echo "OK: cleaned (active stemming was: $$S)"