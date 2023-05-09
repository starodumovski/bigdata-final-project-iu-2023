#!/bin/bash

echo "Preprocess ..."
bash ./scripts/preprocess.sh

echo "Stage 1 ..."
bash ./scripts/stage1.sh

echo "Stage 2 ..."
bash ./scripts/stage2.sh

echo "Complited"
