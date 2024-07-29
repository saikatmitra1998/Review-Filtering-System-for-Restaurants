# Variables
IMAGE_NAME = review_ingestion
DATA_DIR = $(shell pwd)/data
OUTPUT_DIR = $(shell pwd)/output
ALLOWED_REVIEW_AGE = 3
REPLACEMENT = "****"
MIN_RATING = 0
MAX_RATING = 10

# Default target to build the Docker image
build:
	docker build -t $(IMAGE_NAME) .

# Target to run the Docker container
run: build
	docker run --rm -v $(DATA_DIR):/app/data -v $(OUTPUT_DIR):/app/output $(IMAGE_NAME) \
		--input /app/data/reviews.jsonl \
		--inappropriate_words /app/data/inappropriate_words.txt \
		--output /app/output/processed_reviews.jsonl \
		--aggregations /app/output/aggregations.jsonl \
		--discarded /app/output/discarded_reviews.jsonl \
		--review_schema /app/schemas/review.json \
		--aggregation_schema /app/schemas/aggregation.json \
		--years $(ALLOWED_REVIEW_AGE) \
		--replacement $(REPLACEMENT) \
		--min_rating $(MIN_RATING) \
		--max_rating $(MAX_RATING)

# Clean target to remove Docker image
clean:
	docker rmi $(IMAGE_NAME)

# Target to remove the output files
clean-output:
	rm -rf output/*