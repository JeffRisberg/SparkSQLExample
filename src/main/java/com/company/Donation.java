package com.company;

import java.io.Serializable;
import java.sql.Date;

/**
 * @author Jeff Risberg
 * @since 10/29/17
 */
public class Donation implements Serializable {
    private Long id;
    private String charityName;
    private String charityCategory;
    private Date dateCompleted;
    private float amount;
    private String donorFirstName;
    private String donorLastName;

    public Donation() {
    }

    public Donation(Long id, String charityName, String charityCategory, Date dateCompleted, float amount, String donorFirstName, String donorLastName) {
        this.id = id;
        this.charityName = charityName;
        this.charityCategory = charityCategory;
        this.dateCompleted = dateCompleted;
        this.amount = amount;
        this.donorFirstName = donorFirstName;
        this.donorLastName = donorLastName;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Date getDateCompleted() {
        return dateCompleted;
    }

    public void setDateCompleted(Date dateCompleted) {
        this.dateCompleted = dateCompleted;
    }

    public String getCharityName() {
        return charityName;
    }

    public void setCharityName(String charityName) {
        this.charityName = charityName;
    }

    public String getCharityCategory() {
        return charityCategory;
    }

    public void setCharityCategory(String charityCategory) {
        this.charityCategory = charityCategory;
    }

    public float getAmount() {
        return amount;
    }

    public void setAmount(float amount) {
        this.amount = amount;
    }


    public String getDonorFirstName() {
        return donorFirstName;
    }

    public void setDonorFirstName(String donorFirstName) {
        this.donorFirstName = donorFirstName;
    }

    public String getDonorLastName() {
        return donorLastName;
    }

    public void setDonorLastName(String donorLastName) {
        this.donorLastName = donorLastName;
    }
}