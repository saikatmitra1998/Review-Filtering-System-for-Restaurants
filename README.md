# Review Filtering System

The Review Filtering System processes restaurant reviews, filters out inappropriate content, validates reviews against a schema, calculates aggregations, and writes the results to various output files.

## Features

1. **Load and Validate Reviews**: Reads reviews from a JSONL file and validates them against a given schema.
2. **Filter Old Reviews**: Filters out reviews older than a specified number of years.
3. **Replace Inappropriate Words**: Replaces inappropriate words in the review text with a specified replacement string.
4. **Calculate Aggregations**: Computes aggregation metrics like review count, average rating, average review length, and review age statistics for each restaurant.
5. **Save Results**: Writes the processed reviews, aggregations, discarded reviews, and invalid reviews to respective JSONL files.

## Prerequisites

- Docker
- Make

Please note that Python, Spark and jsonschema are being installed by Docker.

## Setup

### Using Makefile

A Makefile is provided to simplify the build and run process.

1. **Build the Docker Image**:
    ```sh
    make build
    ```

2. **Run the Docker Container**:
    ```sh
    make run
    ```

The `Makefile` includes the following targets:
- `build`: Builds the Docker image.
- `run`: Runs the Docker container with the specified volume mappings and arguments.
- `clean`: Removes the Docker image.
- `clean-output`: Removes all files in the `output` directory.

## Arguments in the makefile

- `--input`: Path to the JSONL file containing the reviews.
- `--inappropriate_words`: Path to the text file containing inappropriate words.
- `--output`: Path to the JSONL file to write the processed reviews.
- `--aggregations`: Path to the JSONL file to write the aggregations.
- `--discarded`: Path to the JSONL file to write the discarded reviews.
- `--review_schema`: Path to the schema file for reviews JSON.
- `--aggregation_schema`: Path to the schema file for aggregations JSON.
- `--allowed_review_age`: Number of years to filter reviews (default: 3).
- `--replacement`: Replacement string for inappropriate words (default: "****").
- `--min_rating` : Minimum valid rating (default: 0).
- `--max_rating` : Maximum valid rating (default: 10).

## Output Files

- `processed_reviews.jsonl`: Contains the valid reviews after processing.
- `aggregations.jsonl`: Contains the aggregation metrics for each restaurant.
- `discarded_reviews.jsonl`: Contains the reviews discarded due to inappropriate content.
- `invalid_reviews.jsonl`: Contains the reviews that did not adhere to the schema.

## Logging

The script logs its progress and key actions at the `INFO` level, including:
- Loading inappropriate words.
- Loading and validating reviews.
- Filtering recent reviews.
- Processing inappropriate words.
- Writing output files.
