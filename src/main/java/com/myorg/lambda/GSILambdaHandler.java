package com.myorg.lambda;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
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

public class GSILambdaHandler implements RequestHandler<Object, Object>{

    private DynamoDBMapper mapper;

  @Override
  public Object handleRequest(Object event, Context context)
  {	  
	  JSONObject response = new JSONObject();
	  String newYork = "New York";
      String gsiIndex = "FloodDangerGSIIndex";
      String desiredFloodDanger = "Medium";
	  boolean scanForward = true;

	  try
	  {
            setupDynamoDB();
            List<City> citiesInNewYork = getCitiesForState(newYork, gsiIndex, desiredFloodDanger, scanForward);
            buildResponse(response, citiesInNewYork, desiredFloodDanger, gsiIndex, scanForward);
	  }
	  catch (Exception exc)
	  {
		  return error(response, exc);
	  }

	  return ok(response);
  }

    private void setupDynamoDB() {
        AmazonDynamoDB  client = AmazonDynamoDBClientBuilder.standard()
                .withRegion(Regions.DEFAULT_REGION).build();

        String tableName = System.getenv("CITYDYNAMODBTABLE");
        DynamoDBMapperConfig mapperConfig = DynamoDBMapperConfig.builder()
                .withTableNameOverride(TableNameOverride.withTableNameReplacement(tableName))
                .build();

        this.mapper = new DynamoDBMapper(client,mapperConfig);
    }

    private List<City> getCitiesForState(String state, String gsiIndex, String desiredFloodDanger, boolean scanForward) {
        //Please note that for GSI you can only use the equal operator and not greater than or less than
        //or you will get an message like : Query key condition not supported (Service: AmazonDynamoDBv2; Status Code: 400; Error Code: ValidationException;
        Map<String, AttributeValue> eav = Map.of(":v_floodDanger", new AttributeValue().withS(desiredFloodDanger));
        String filterExpression = "floodDanger = :v_floodDanger";

        //setting sorting to be forward by the GSI sort key of elevationMetres
        //withScanIndexForward (false is descending, true is ascending) based on the sort key(elevationMetres) of the GSI
        DynamoDBQueryExpression<City> queryExpression = new DynamoDBQueryExpression<City>()
                .withIndexName(gsiIndex)
                .withConsistentRead(false)
                .withKeyConditionExpression(filterExpression)
                .withExpressionAttributeValues(eav)
                .withScanIndexForward(scanForward);

        return mapper.query(City.class, queryExpression);
    }

    private void buildResponse(JSONObject jsonResponse, List<City> cities, String desiredFloodDanger, String gsiIndex, boolean scanForward) {
        jsonResponse.put("numberOfCitiesWithFloodDanger of "+desiredFloodDanger, cities.size());
        jsonResponse.put("GSI Index", gsiIndex);
        jsonResponse.put("GSI PrimaryKey", "floodDanger");
        jsonResponse.put("GSI SortKey", "elevationMetres");
        jsonResponse.put("scanIndexForward", scanForward);

        JSONArray cityArray = new JSONArray();
        cityArray.addAll(cities);
        jsonResponse.put("Cities", cityArray);
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