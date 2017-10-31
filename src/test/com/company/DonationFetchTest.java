package com.company;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DonationFetchTest {

    private SparkSession spark;

    @Before
    public void setupSpark() throws Exception {
        spark = DonationAggregation.setupSpark();
    }

    @Test
    public void fetchFromLocal() {
        String fileName = "donations.json";

        Dataset<Row> donationsDF = spark.read().json("data/" + fileName);

        assertEquals(32L, donationsDF.count());
    }

    @Test
    public void fetchFromS3() {
        String s3Bucket = "jeffrisberg2017";
        String fileName = "donations.json";

        Dataset<Row> donationsDF = spark.read().json("s3n://" + s3Bucket + "/" + fileName);

        assertEquals(32L, donationsDF.count());
    }

    @Test
    public void addDateHourColumns() {
        String fileName = "donations.json";

        Dataset<Row> donationsDF = spark.read().json("data/" + fileName);

        Dataset donationsWithDateAndTimeDF = DonationAggregation.addDateHourColumns(donationsDF, "dateCompleted");

        List columnNames = Arrays.asList(donationsWithDateAndTimeDF.columns());

        assertTrue(columnNames.contains("date"));
        assertTrue(columnNames.contains("hour"));
    }
}
