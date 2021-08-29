package com.myorg.dynamodb;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIndexHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIndexRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

@DynamoDBTable(tableName="City")
public class City {

    private String state;
    private String city;
    private Integer founded;
    private Integer population;
    private Integer elevationMetres;
    private String floodDanger;
    
    //Maps a class property to the partition key of the table.
    @DynamoDBHashKey(attributeName="state")
    public String getState() { return state; }
    public void setState(String state) {this.state = state; }

    //Maps a class property to the sort key of the table.
    @DynamoDBRangeKey(attributeName="city")
    public String getCity() {return city; }
    public void setCity(String city) { this.city = city; }

    //Maps a class property to the sort key of a global secondary index or a local secondary index.
    @DynamoDBIndexRangeKey(localSecondaryIndexName = "FoundedLSIPopulationIndex",attributeName="founded")
    public Integer getFounded() { return founded ; }
    public void setFounded(Integer founded) { this.founded = founded; }

    //Maps a property to a table attribute. 
    @DynamoDBAttribute(attributeName="population")
    public Integer getPopulation() { return population ; }
    public void setPopulation(Integer population) { this.population = population; }

    //Maps a class property to the partition key of a global secondary index.
    @DynamoDBIndexHashKey(globalSecondaryIndexName = "FloodDangerGSIIndex", attributeName="floodDanger")
    public String getFloodDanger() { return floodDanger ; }
    public void setFloodDanger(String floodDanger) { this.floodDanger = floodDanger; }

    //Maps a class property to the sort key of a global secondary index or a local secondary index.
    @DynamoDBIndexRangeKey(globalSecondaryIndexName = "FloodDangerGSIIndex", attributeName="elevationMetres")
    public Integer getElevationMetres() { return elevationMetres ; }
    public void setElevationMetres(Integer elevationMetres) { this.elevationMetres = elevationMetres; }

    
    public String toString()
    {
    	String val = "City(";
    	val += this.state+",";
    	val += this.city+")";
    	val += "founded:"+this.founded+",";
    	val += "population:"+this.population+",";
    	val += "floodDanger:"+this.floodDanger+",";
    	val += "elevationMetres:"+this.elevationMetres+".";
    	return val;
    }
    
}