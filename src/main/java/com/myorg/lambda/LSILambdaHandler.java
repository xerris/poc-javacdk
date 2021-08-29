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
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.myorg.dynamodb.City;

public class LSILambdaHandler implements RequestHandler<Object, Object>{

    private DynamoDBMapper mapper;

	@SuppressWarnings({ "unchecked" })
  @Override
  public Object handleRequest(Object event, Context context)
  {
	  JSONObject response = new JSONObject();
	  try
	  {
		  setupDynamoDB();
		  String state = "Alberta";
		  String lsiIndex = "FoundedLSIPopulationIndex";
		  List<City> cities = getCitiesFor(state, lsiIndex, true);

	      response.put("numberOfCitiesWithState EQ "+state, cities.size());
	      response.put("LSI Index", lsiIndex);
	      response.put("Partition Key", "state");
	      response.put("LSI SortKey", "founded");
	      response.put("scanIndexForward", true);
	      JSONArray cityArray = new JSONArray();
	      cityArray.addAll(cities);
	      response.put("Cities", cityArray);
	
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
	private List<City> getCitiesFor(String state, String lsiIndex, boolean scanForward) {

		// Set up an alias for the partition key name in case it's a reserved word
		Map<String,String> attrNameAlias = Map.of("#state", "state");

		// Set up mapping of the partition name with the value
		Map<String, AttributeValue> attrValues =  Map.of(":state", new AttributeValue().withS(state));

		boolean scanIndexForward = true;
		DynamoDBQueryExpression<City> queryExpression = new DynamoDBQueryExpression<>();
		queryExpression.setIndexName(lsiIndex);
		queryExpression.withKeyConditionExpression("#state = :state");
		queryExpression.setExpressionAttributeNames(attrNameAlias);
		queryExpression.setExpressionAttributeValues(attrValues);

		//withScanIndexForward (false is descending, true is ascending) based on the sort key of this LSI(founded) of the GSI
		queryExpression.withScanIndexForward(scanForward);

		return mapper.query(City.class, queryExpression);
	}


	private DynamoDBMapper createMapper() {
		AmazonDynamoDB  client = AmazonDynamoDBClientBuilder.standard()
				.withRegion(Regions.DEFAULT_REGION).build();
		String tableName = System.getenv("CITYDYNAMODBTABLE");
		DynamoDBMapperConfig mapperConfig = DynamoDBMapperConfig.builder()
				.withTableNameOverride(TableNameOverride.withTableNameReplacement(tableName))
				.build();

		return new DynamoDBMapper(client,mapperConfig);
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