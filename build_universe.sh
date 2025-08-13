#!/usr/bin/env bash
set -euo pipefail

# 1) Download official symbol directories
curl -s https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt -o nasdaqlisted.txt
curl -s https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt  -o otherlisted.txt

# 2) Extract tickers, drop test issues and ETFs (column positions per NASDAQ docs)
# nasdaqlisted.txt columns:
#   1 Symbol | 2 Security Name | 3 Market Category | 4 Test Issue | 5 Financial Status |
#   6 CQS Symbol | 7 ETF | 8 Round Lot Size | 9 NASDAQ Official Symbol | 10 NextShares
awk -F '|' 'NR>1 && $1!="Symbol" && $4=="N" && $7!="Y" {print $1}' nasdaqlisted.txt > tmp1.txt

# otherlisted.txt columns:
#   1 ACT Symbol | 2 Security Name | 3 Exchange | 4 CQS Symbol | 5 ETF |
#   6 Round Lot Size | 7 Test Issue | 8 NASDAQ Symbol
awk -F '|' 'NR>1 && $1!="ACT Symbol" && $7=="N" && $5!="Y" {print $1}' otherlisted.txt > tmp2.txt

# 3) Merge & dedupe
cat tmp1.txt tmp2.txt | sort -u > universe_all.txt
rm -f tmp1.txt tmp2.txt

# 4) Keep common equities: symbols with letters only (drop tickers containing . - ^ ~ / etc.)
#    (This will exclude things like BRK.B; we can map those to Yahoo format later if you want)
grep -E '^[A-Z]+$' universe_all.txt > universe.txt || cp universe_all.txt universe.txt

# 5) Show count
wc -l universe.txt