#!/bin/bash
# start_relayer.sh

echo "🚀 Starting Real-time Data Relayer"
echo "================================="

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Check if data files exist
echo -e "\n${YELLOW}🔍 Checking for data files...${NC}"

REDDIT_FILES=("data/raw/reddit_wsb.csv" "data/simulate/reddit_sim.csv" "data/train/reddit_train.csv")
STOCK_FILES=("data/raw/stock_prices.csv" "data/simulate/stock_sim.csv" "data/train/stock_train.csv")

reddit_file=""
stock_file=""

for file in "${REDDIT_FILES[@]}"; do
  if [ -f "$file" ]; then
    reddit_file="$file"
    echo -e "${GREEN}✅ Found Reddit data: $file${NC}"
    break
  fi
done

for file in "${STOCK_FILES[@]}"; do
  if [ -f "$file" ]; then
    stock_file="$file"
    echo -e "${GREEN}✅ Found Stock data: $file${NC}"
    break
  fi
done

if [ -z "$reddit_file" ] || [ -z "$stock_file" ]; then
  echo -e "${RED}❌ Missing data files!${NC}"
  echo "Please ensure you have both Reddit and Stock CSV files in one of these locations:"
  echo "  - data/raw/"
  echo "  - data/simulate/"
  echo "  - data/train/"
  exit 1
fi

# Check if Kafka is running
echo -e "\n${YELLOW}🌐 Checking Kafka...${NC}"
if ! docker-compose ps kafka | grep -q "Up"; then
  echo -e "${YELLOW}⚠️ Kafka is not running. Starting it now...${NC}"
  docker-compose up -d kafka
  echo "Waiting 15 seconds for Kafka to start..."
  sleep 15
else
  echo -e "${GREEN}✅ Kafka is running${NC}"
fi

# Create logs directory
mkdir -p logs

# Start the relayer
echo -e "\n${YELLOW}📡 Starting Data Relayer...${NC}"
echo -e "${GREEN}Reddit file: $reddit_file${NC}"
echo -e "${GREEN}Stock file: $stock_file${NC}"
echo -e "${YELLOW}Speed: 2 hours of data per second${NC}"

python orchestration/relayer_simulator.py \
  --mode continuous \
  --speed 7200 \
  --reddit-file "$reddit_file" \
  --stock-file "$stock_file"

# If relayer exits
echo -e "\n${YELLOW}🏁 Relayer stopped${NC}"
