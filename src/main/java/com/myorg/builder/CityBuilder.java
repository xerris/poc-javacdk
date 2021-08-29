package com.myorg.builder;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.myorg.dynamodb.City;

public class CityBuilder {
    private City city;
    private int inserted;
    private DynamoDBMapper mapper;
    private DynamoDBMapperConfig mapperConfig;

    public CityBuilder(DynamoDBMapper mapper, DynamoDBMapperConfig mapperConfig) {
        this.mapper = mapper;
        this.mapperConfig = mapperConfig;
        city = new City();
    }

    public DynamoDBMapper getMapper() {
        return this.mapper;
    }

    public CityBuilder state(String state) {
        city.setState(state);
        return this;
    }

    public CityBuilder name(String name) {
        city.setCity(name);
        return this;
    }

    public CityBuilder population(int population) {
        city.setPopulation(population);
        return this;
    }

    public CityBuilder floodDanger(String danger) {
        city.setFloodDanger(danger);
        return this;
    }

    public CityBuilder elevation(int elevation) {
        city.setElevationMetres(elevation);
        return this;
    }

    public CityBuilder yearFounded(int yearFounded) {
        city.setFounded(yearFounded);
        return this;
    }

    public int count() {
        return inserted;
    }

    public void save() {
        mapper.save(city);
        city = new City();
        inserted+=1;
    }
}