package com.company;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author Jeff Risberg
 * @since 10/30/17
 */
public class Part1 {

    static Dataset<Row> addDateHourColumns(Dataset<Row> input, String dateFieldName) {
        Dataset<Row> result = input.withColumn("hour", functions.hour(input.col(dateFieldName)))
                .withColumn("date",functions.to_date(input.col(dateFieldName)));

        return result;
    }

    static Dataset<Row> groupCountAndAmountFunc(Dataset<Row> input) {
        RelationalGroupedDataset result = input.groupBy("date", "hour");

        return result.agg(
                functions.count("id").as("count"),
                functions.sum("amount").as("amount"));
    }

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("SparkSQLExample")
                .master("local[4]")
                .getOrCreate();

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        // Create an RDD of Donation objects from a text file
        JavaRDD<Donation> donationRDD = spark.read()
                .textFile("./data/donations.txt")
                .javaRDD()
                .map(new Function<String, Donation>() {
                    @Override
                    public Donation call(String line) throws Exception {
                        String[] parts = line.split(",");

                        Date parsedDate = dateFormat.parse(parts[3].trim());
                        Timestamp timestamp = new java.sql.Timestamp(parsedDate.getTime());

                        Donation donation = new Donation();
                        donation.setId(Long.parseLong(parts[0].trim()));
                        donation.setCharityName(parts[1].trim());
                        donation.setCharityCategory(parts[2].trim());
                        donation.setDateCompleted(timestamp);
                        donation.setAmount(Float.parseFloat(parts[4].trim()));
                        donation.setDonorFirstName(parts[5].trim());
                        donation.setDonorLastName(parts[6].trim());

                        return donation;
                    }
                });

        // Apply a schema to an RDD of JavaBeans to get a DataFrame
        Dataset<Row> donationsDF = spark.createDataFrame(donationRDD, Donation.class);

        // Register the DataFrame as a temporary view
        donationsDF.createOrReplaceTempView("donations");

        // Part 1
        // SQL statements can be run by using the sql methods provided by spark
        Dataset<Row> donationsRedCrossDF = spark.sql("SELECT * FROM donations where charityName = 'Red Cross'");

        donationsRedCrossDF.show();

        // Part 2
        donationsDF.groupBy("charityCategory").agg(
                functions.count("id").as("count"),
                functions.sum("amount").as("amount")).show();

        Dataset<Row> donations2DF = donationsDF.withColumn("hour", functions.hour(donationsDF.col("dateCompleted")))
                .withColumn("date",functions.to_date(donationsDF.col("dateCompleted")));

        donations2DF.show();

        Dataset x = donations2DF.groupBy("date", "hour").agg(
                functions.count("id").as("count"),
                functions.sum("amount").as("amount"));

        x.show();

        Dataset<Row> yy = addDateHourColumns(donationsDF, "dateCompleted");
        groupCountAndAmountFunc(yy).show();

        // Part 3
        Dataset<Row> qwerty = spark.sql("SELECT * FROM donations where charityName = 'Red Cross'");

        groupCountAndAmountFunc(addDateHourColumns(qwerty, "dateCompleted")).show();

        spark.stop();
    }
}
