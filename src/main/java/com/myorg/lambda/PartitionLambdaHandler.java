package com.myorg.lambda;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.jetbrains.annotations.NotNull;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.TableNameOverride;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;

import com.myorg.dynamodb.City;

public class PartitionLambdaHandler implements RequestHandler<Object, Object> {

    private DynamoDBMapper mapper;

    @SuppressWarnings({"unchecked"})
    @Override
    public Object handleRequest(Object event, Context context) {
        JSONObject response = new JSONObject();
        String newYork = "New York";
        boolean scanForward = true;

        try {
            setupDynamoDB();
            List<City> newYorkCities = getCitiesForState(newYork, scanForward);
            buildResponse(response, scanForward, newYork, newYorkCities);

        } catch (Exception exc) {
            return error(response, exc);
        }
        return ok(response);
    }

    @NotNull
    private void setupDynamoDB() {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withRegion(Regions.DEFAULT_REGION).build();
        String tableName = System.getenv("CITYDYNAMODBTABLE");
        DynamoDBMapperConfig mapperConfig = DynamoDBMapperConfig.builder()
                .withTableNameOverride(TableNameOverride.withTableNameReplacement(tableName))
                .build();
        this.mapper = new DynamoDBMapper(client, mapperConfig);
    }


    private List<City> getCitiesForState(String state, boolean scanForward) {
        HashMap<String, String> queryAttributes = new HashMap<>();
        queryAttributes.put("#state", "state");

        HashMap<String, AttributeValue> searchValue = new HashMap<>();
        searchValue.put(":state", new AttributeValue().withS(state));

        DynamoDBQueryExpression<City> queryExpression = new DynamoDBQueryExpression<>();
        queryExpression.withKeyConditionExpression("#state = :state");   // state is reserved.  use an alias in this case
        queryExpression.setExpressionAttributeNames(queryAttributes);
        queryExpression.setExpressionAttributeValues(searchValue);
        queryExpression.setScanIndexForward(scanForward);

        return mapper.query(City.class, queryExpression);
    }

    private void buildResponse(JSONObject response, boolean scanForward, String state, List<City> cities) {
        response.put("state", state);
        response.put("numberOfCitiesInState " + state, cities.size());
        response.put("partition key", "state");
        response.put("sort key", "city");
        response.put("scanForward", scanForward);
        JSONArray cityArray = new JSONArray();
        cityArray.addAll(cities);
        response.put("Cities", cityArray);
    }


    private APIGatewayProxyResponseEvent ok(JSONObject response) {
        return new APIGatewayProxyResponseEvent()
                .withStatusCode(200)
                .withBody(response.toJSONString())
                .withIsBase64Encoded(false);
    }

    private APIGatewayProxyResponseEvent error(JSONObject response, Exception exc) {
        String exceptionString = String.format("error: %s: %s", exc.getMessage(), Arrays.toString(exc.getStackTrace()));
        response.put("Exception", exceptionString);
        return new APIGatewayProxyResponseEvent()
                .withStatusCode(500)
                .withBody(response.toJSONString())
                .withIsBase64Encoded(false);
    }
}