package com.myorg.lambda;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.TableNameOverride;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.myorg.dynamodb.City;

public class ScanLambdaHandler implements RequestHandler<Object, Object>{

	private final DynamoDBMapper mapper;

	public ScanLambdaHandler() {
		this.mapper = createMapper();
	}


	@SuppressWarnings({ "unchecked" })
  @Override
  public Object handleRequest(Object event, Context context)
  {	  
	  JSONObject response = new JSONObject();
	  try
	  {		//Let's find all the cities with a population greater than one million
		  int population = 1000000;

			List<City> cities = getMetropolitanCities(population, response);

	        response.put("numberOfCitiesWithPopulation GT "+population, cities.size());

			JSONArray cityArray = new JSONArray();
			cityArray.addAll(cities);
    		response.put("cities", cityArray);

	  }
	  catch (Exception exc)
	  {
		  String exceptionString = "" + exc.getMessage()+":"+exc.getStackTrace();
		  response.put("Exception", exceptionString);
	  }
	  //response.put("Trace", trace);

	  return new APIGatewayProxyResponseEvent()
            .withStatusCode(200)
            .withBody(response.toJSONString())
            .withIsBase64Encoded(false);
  }

	private List<City> getMetropolitanCities(int population, JSONObject response) {

		//We are using a Scan which passes through all the DynamoDb Records with is not very efficient
		//One should utilize partitions/gsi and lsi indexes to make searching more efficient/effective

		Map<String, AttributeValue> eav =Map.of(":val1", new AttributeValue().withN(String.valueOf(population)));
		String filterExpression = "population > :val1";

		response.put("val1", String.valueOf(population));
		response.put("filterExpression", filterExpression);

		DynamoDBScanExpression scanExpression = new DynamoDBScanExpression()
				.withFilterExpression(filterExpression)
				.withExpressionAttributeValues(eav);

		return this.mapper.scan(City.class, scanExpression);
	}

	private DynamoDBMapper createMapper() {
		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
				.withRegion(Regions.DEFAULT_REGION)
				.build();

		//Setting the DynamoDB the City DynamoDB tablename (See JavaCdkStack)
		String tableName = System.getenv("CITYDYNAMODBTABLE");
		DynamoDBMapperConfig mapperConfig = DynamoDBMapperConfig.builder()
				.withTableNameOverride(TableNameOverride.withTableNameReplacement(tableName))
				.build();
		return new DynamoDBMapper(client,mapperConfig);
	}
}