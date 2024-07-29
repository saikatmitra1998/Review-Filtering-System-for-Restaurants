import argparse
import json
import logging
import re
from datetime import datetime, timedelta
from jsonschema import validate, ValidationError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, udf, avg, length, min as spark_min, max as spark_max, count, to_timestamp, datediff, current_date, concat_ws
from pyspark.sql.types import StringType, FloatType

def load_schema(schema_path):
    """
    Load the JSON schema from a file.
    """
    with open(schema_path, 'r') as file:
        return json.load(file)

def validate_review_schema(review, schema):
    """
    Validate a review against the JSON schema.
    
    Parameters:
    review (dict): The review data to be validated.
    schema (dict): The JSON schema to validate against.
    
    Returns:
    bool: True if the review is valid, False otherwise.
    """
    try:
        validate(instance=review, schema=schema)
        return True
    except ValidationError:
        return False

def load_inappropriate_words(file_path):
    """
    Load inappropriate words from a file into a set.
    """
    with open(file_path, 'r') as f:
        return set(word.strip().lower() for word in f.readlines())

def replace_inappropriate_words(text, inappropriate_words, replacement):
    """
    Replace inappropriate words in the text with a replacement string.
    
    Parameters:
    text (str): The original review text.
    inappropriate_words (set): A set of inappropriate words to be replaced.
    replacement (str): The string to replace inappropriate words with.
    
    Returns:
    str: The cleaned text with inappropriate words replaced.
    """
    if text is None:
        return text
    words = re.findall(r'\b\w+\b', text)  # Extract words ignoring punctuation
    cleaned_words = [replacement if word.lower() in inappropriate_words else word for word in words]
    return ' '.join(cleaned_words)

def calculate_inappropriate_ratio(text, inappropriate_words):
    """
    Calculate the ratio of inappropriate words in the text.
    
    Parameters:
    text (str): The original review text.
    inappropriate_words (set): A set of inappropriate words.
    
    Returns:
    float: The ratio of inappropriate words in the text.
    """
    if text is None:
        return 0
    words = re.findall(r'\b\w+\b', text)  # Extract words ignoring punctuation
    inappropriate_count = sum(1 for word in words if word.lower() in inappropriate_words)
    return inappropriate_count / len(words) if words else 0

def write_jsonl(data, path):
    """
    Write a list of dictionaries or Row objects to a JSONL file.
    
    Parameters:
    data (list): The list of dictionaries or Row objects to be written.
    path (str): The output file path.
    """
    def convert_to_serializable(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return obj

    with open(path, 'w') as f:
        for item in data:
            if not isinstance(item, dict):
                item = item.asDict()
            serializable_item = {k: convert_to_serializable(v) for k, v in item.items()}
            f.write(json.dumps(serializable_item) + '\n')

def main(args):
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Initialize Spark session
    spark = SparkSession.builder.appName("ReviewIngestionSystem").getOrCreate()

    # Load inappropriate words
    inappropriate_words = load_inappropriate_words(args.inappropriate_words)
    logger.info("Loaded inappropriate words.")

    # Load schemas
    review_schema = load_schema(args.review_schema)
    aggregation_schema = load_schema(args.aggregation_schema)

    # Define UDFs (User Defined Functions)
    replace_inappropriate_words_udf = udf(lambda text: replace_inappropriate_words(text, inappropriate_words, args.replacement), StringType())
    calculate_inappropriate_ratio_udf = udf(lambda text: calculate_inappropriate_ratio(text, inappropriate_words), FloatType())

    # Read reviews from input file
    reviews_df = spark.read.json(args.input)
    logger.info(f"Loaded reviews: {reviews_df.count()} rows")

    # Validate reviews against the schema
    valid_reviews = []
    invalid_reviews = []
    for row in reviews_df.collect():
        review = row.asDict()
        if validate_review_schema(review, review_schema):
            valid_reviews.append(review)
        else:
            invalid_reviews.append(review)

    logger.info(f"Filtered invalid reviews: {len(invalid_reviews)} rows")
    logger.info(f"Valid reviews: {len(valid_reviews)} rows")

    valid_reviews_df = spark.createDataFrame(valid_reviews)
    invalid_reviews_df = spark.createDataFrame(invalid_reviews)

    # Filter out reviews older than the specified number of years
    filter_date = datetime.now() - timedelta(days=args.years * 365)
    valid_reviews_df = valid_reviews_df.withColumn("publishedAt", to_timestamp(col("publishedAt")))
    valid_reviews_df = valid_reviews_df.filter(col('publishedAt') >= filter_date.isoformat())
    logger.info(f"Filtered recent reviews: {valid_reviews_df.count()} rows")

    # Filter out reviews with invalid ratings
    # valid_reviews_df = valid_reviews_df.filter((col('rating') >= 0) & (col('rating') <= 10))
    valid_reviews_df = valid_reviews_df.filter((col('rating') >= args.min_rating) & (col('rating') <= args.max_rating))
    logger.info(f"Filtered reviews with valid ratings: {valid_reviews_df.count()} rows")

    # Drop duplicate reviews based on restaurantId and reviewId
    valid_reviews_df = valid_reviews_df.dropDuplicates(['restaurantId', 'reviewId', 'text', 'publishedAt', 'rating'])

    # Replace inappropriate words and calculate the ratio of inappropriate words
    valid_reviews_df = valid_reviews_df.withColumn('clean_review_text', replace_inappropriate_words_udf(col('text')))
    valid_reviews_df = valid_reviews_df.withColumn('inappropriate_ratio', calculate_inappropriate_ratio_udf(col('text')))
    logger.info(f"Processed inappropriate words.")

    # Separate valid and discarded reviews based on inappropriate word ratio
    valid_reviews_final_df = valid_reviews_df.filter(col('inappropriate_ratio') <= 0.2)
    discarded_reviews_df = valid_reviews_df.filter(col('inappropriate_ratio') > 0.2)
    logger.info(f"Valid reviews: {valid_reviews_final_df.count()} rows")
    logger.info(f"Discarded reviews: {discarded_reviews_df.count()} rows")

    # Calculate aggregation metrics for valid reviews
    valid_reviews_final_df = valid_reviews_final_df.withColumn('review_length', length(col('clean_review_text')))
    valid_reviews_final_df = valid_reviews_final_df.withColumn('review_age', datediff(current_date(), col('publishedAt')))

    aggregations_df = valid_reviews_final_df.groupBy('restaurantId').agg(
        count('*').alias('reviewCount'),
        avg('rating').alias('averageRating'),
        avg('review_length').alias('averageReviewLength'),
        spark_min('review_age').alias('oldest_review_age_in_days'),
        spark_max('review_age').alias('newest_review_age_in_days'),
        avg('review_age').alias('average_review_age_in_days')
    )
    logger.info(f"Aggregations calculated.")

    # Write the valid reviews, aggregations, and discarded reviews to respective output files
    if valid_reviews_final_df.count() > 0:
        write_jsonl(valid_reviews_final_df.collect(), args.output)
        logger.info(f"Written valid reviews to {args.output}")
    else:
        logger.warning(f"No valid reviews to write")

    if aggregations_df.count() > 0:
        aggregations_rdd = aggregations_df.rdd.map(lambda row: row.asDict())
        aggregations_list = aggregations_rdd.collect()
        structured_aggregations = [
            {
                "restaurantId": agg["restaurantId"],
                "reviewCount": agg["reviewCount"],
                "averageRating": agg["averageRating"],
                "averageReviewLength": agg["averageReviewLength"],
                "reviewAge": {
                    "oldest": agg["oldest_review_age_in_days"],
                    "newest": agg["newest_review_age_in_days"],
                    "average": agg["average_review_age_in_days"]
                }
            } for agg in aggregations_list
        ]
        write_jsonl(structured_aggregations, args.aggregations)
        logger.info(f"Written aggregations to {args.aggregations}")
    else:
        logger.warning(f"No aggregations to write")

    if discarded_reviews_df.count() > 0:
        # Apply the UDF to replace inappropriate words in the discarded reviews before writing them
        discarded_reviews_df = discarded_reviews_df.withColumn('text', replace_inappropriate_words_udf(col('text')))
        write_jsonl(discarded_reviews_df.collect(), args.discarded)
        logger.info(f"Written discarded reviews to {args.discarded}")
    else:
        logger.warning(f"No discarded reviews to write")

    # Write invalid reviews to a separate file in the output folder
    if invalid_reviews_df.count() > 0:
        write_jsonl(invalid_reviews_df.collect(), f"{args.output.rsplit('/', 1)[0]}/invalid_reviews.jsonl")
        logger.info(f"Written invalid reviews to {args.output.rsplit('/', 1)[0]}/invalid_reviews.jsonl")
    else:
        logger.warning(f"No invalid reviews to write")

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Review Ingestion System")
    parser.add_argument('--input', required=True, help="Path to the JSONL file containing the reviews")
    parser.add_argument('--inappropriate_words', required=True, help="Path to the text file containing inappropriate words")
    parser.add_argument('--output', required=True, help="Path to the JSONL file to write the processed reviews")
    parser.add_argument('--aggregations', required=True, help="Path to the JSONL file to write the aggregations")
    parser.add_argument('--discarded', required=True, help="Path to the JSONL file to write the discarded reviews")
    parser.add_argument('--review_schema', required=True, help="Path to the schema file for reviews JSON")
    parser.add_argument('--aggregation_schema', required=True, help="Path to schema file for aggregations JSON")
    parser.add_argument('--years', type=int, default=3, help="Number of years to filter reviews")
    parser.add_argument('--replacement', type=str, default='****', help="Replacement string for inappropriate words")
    parser.add_argument('--min_rating', type=float, default=0, help="Minimum valid rating value")
    parser.add_argument('--max_rating', type=float, default=10, help="Maximum valid rating value")
    args = parser.parse_args()

    main(args)