package com.company;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.sql.Date;

/**
 * @author Jeff Risberg
 * @since 10/30/17
 */
public class Part1 {
    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("SparkSQLExample")
                .master("local[4]")
                .getOrCreate();

        /*
        // Create a dataframe
        Dataset<Row> df = spark.read().option("header", "true").csv("./data/peopleWithHeader.txt");
        df.show();

        // This is relatively fast
        Dataset<Row> dfs = spark.read().json("./data/employee.json");
        dfs.show();
        dfs.printSchema();

        // this is faster
        dfs.filter(dfs.col("age").gt(23)).show();

        // This seems to take quite a while, perhaps because GroupBy and Count are
        // creating a new RDD, and count requires a collection over the clusters.
        dfs.groupBy("age").count().show();
*/
        // Create an RDD of Donation objects from a text file
        JavaRDD<Donation> donationRDD = spark.read()
                .textFile("./data/donations.txt")
                .javaRDD()
                .map(new Function<String, Donation>() {
                    @Override
                    public Donation call(String line) throws Exception {
                        String[] parts = line.split(",");

                        Donation donation = new Donation();
                        donation.setId(Long.parseLong(parts[0].trim()));
                        donation.setCharityName(parts[1].trim());
                        donation.setCharityCategory(parts[2].trim());
                        donation.setDateCompleted(new Date(117,1,1)); //parts[3].trim());
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

        // SQL statements can be run by using the sql methods provided by spark
        Dataset<Row> donations2DF = spark.sql("SELECT * FROM donations");

        /*
        // The columns of a row in the result can be accessed by field index
        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> teenagerNamesByIndexDF = teenagersDF.map(new MapFunction<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                return "Name: " + row.getString(0);
            }
        }, stringEncoder);
        teenagerNamesByIndexDF.show();

        // or accessed by field name
        Dataset<String> teenagerNamesByFieldDF = teenagersDF.map(new MapFunction<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                return "Name: " + row.<String>getAs("name");
            }
        }, stringEncoder);
        teenagerNamesByFieldDF.show();
*/
        // Summary statistics are also available.
        Dataset<Row> summary = donationsDF.describe();
        summary.show();

        spark.stop();
    }
}
