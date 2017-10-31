package com.company;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Programming Assignment using Spark SQL
 * <p>
 * Data Setup:
 * Uses extracted donation data from JustGiving test cases.  The data fields include:
 * id
 * dateCompleted - dateTime
 * charityName - string
 * charityCode - categorical value based on NTEE code (National Taxonomy Except Entities)
 * amount - double
 * donor information - two strings
 * <p>
 * Assignment parts:
 * - we fetch information as JSON from S3
 * - we execute SQL query against that data
 * - we define function to carry out rollup by date,hour
 * - we execute SQL query against rolledup data, aggregating sum(amount), and count().
 * - this is also applied for specific categorical codes
 * <p>
 * NTEE codes for charities are defined at
 * http://nccs.urban.org/classification/national-taxonomy-exempt-entities
 * <pre>
 * Arts, Culture, and Humanities - A
 * Education - B
 * Environment and Animals - C, D
 * Health - E, F, G, H
 * Human Services - I, J, K, L, M, N, O, P
 * International, Foreign Affairs - Q
 * Public, Societal Benefit - R, S, T, U, V, W
 * Religion Related - X
 * Mutual/Membership Benefit - Y
 * Unknown, Unclassified - Z
 * </pre>
 *
 * @author Jeff Risberg
 * @since 10/30/17
 */
public class DonationAggregation {
    private static final Logger logger = LoggerFactory.getLogger(DonationAggregation.class);

    public static SparkSession setupSpark() {
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkSQLExample")
                .master("local[4]")
                .getOrCreate();

        ProfileCredentialsProvider auth = new ProfileCredentialsProvider();

        String accessKeyId = auth.getCredentials().getAWSAccessKeyId();
        String secretAccessKey = auth.getCredentials().getAWSSecretKey();

        // Create an Dataset of Donation records from a json file
        spark.sparkContext().hadoopConfiguration().set("fs.s3n.awsAccessKeyId", accessKeyId);
        spark.sparkContext().hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", secretAccessKey);

        return spark;
    }

    public static Dataset<Row> addDateHourColumns(Dataset<Row> input, String dateFieldName) {
        Dataset<Row> result = input.withColumn("hour", functions.hour(input.col(dateFieldName)))
                .withColumn("date", functions.to_date(input.col(dateFieldName)));

        return result;
    }

    public static Dataset<Row> aggregateCountAndAmountFunc(Dataset<Row> input) {
        RelationalGroupedDataset result = input.groupBy("date", "hour");

        return result.agg(
                functions.count("id").as("count"),
                functions.sum("amount").as("amount")).orderBy("date", "hour");
    }

    public static void main(String[] args) {
        SparkSession spark = setupSpark();

        String s3Bucket = "jeffrisberg2017";
        String fileName = "donations.json";

        Dataset<Row> donationsDF = spark.read().json("s3n://" + s3Bucket + "/" + fileName);

        donationsDF.describe().show();

        // Register the DataFrame as a temporary view
        donationsDF.createOrReplaceTempView("donations");

        // Part 1
        logger.warn("Part 1 - run query");

        Dataset<Row> donationsRedCrossDF = spark.sql("SELECT * FROM donations where charityName = 'Red Cross'");

        donationsRedCrossDF.show();

        // Part 2
        logger.warn("Part 2 - define and use function to rollup by date/hour and aggregate");

        Dataset<Row> donationsAggregatedDF = addDateHourColumns(donationsDF, "dateCompleted");

        aggregateCountAndAmountFunc(donationsAggregatedDF).show();

        // Part 3
        logger.warn("Part 3 - date/hour aggregration for categorical value");

        Dataset<Row> part3QueryDF = spark.sql("SELECT * FROM donations where charityCategory = 'B'");

        aggregateCountAndAmountFunc(addDateHourColumns(part3QueryDF, "dateCompleted")).show();

        // Part 3a
        logger.warn("Part 3a - summation across categorical values");

        donationsDF.groupBy("charityCategory").agg(
                functions.count("id").as("count"),
                functions.sum("amount").as("amount")).orderBy(functions.desc("amount")).show();

        logger.warn("The majority of donations are in NTEE category 'B', which is 'Education'");
        spark.stop();
    }
}
