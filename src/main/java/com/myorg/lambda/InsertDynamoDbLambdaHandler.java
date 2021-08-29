package com.myorg.lambda;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import com.myorg.builder.CityBuilder;
import org.json.simple.JSONObject;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.TableNameOverride;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.myorg.dynamodb.City;

public class InsertDynamoDbLambdaHandler implements RequestHandler<Object, Object> {

    private final CityBuilder builder;
    private DynamoDBMapper mapper;

    public InsertDynamoDbLambdaHandler() {
        this.builder = createCityBuilder();
        this.mapper = builder.getMapper();
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public Object handleRequest(Object event, Context context) {
        JSONObject response = new JSONObject();

        try {
            String newYork = "New York";
            List<City> cities = getCities(newYork);
            response.put("numberOfCities at start for (" + newYork + ")", cities.size());

            if (cities.size() > 0) return ok(response);

            builder.state("California").name("Los Angeles").yearFounded(1781).population(3792621).elevation(93).floodDanger("Medium").save();
            builder.state("California").name("Sacramento").yearFounded(1854).population(513624).elevation(9).floodDanger("High").save();
            builder.state("California").name("San Diego").yearFounded(1769).population(1386932).elevation(19).floodDanger("High").save();
            builder.state("New York").name("New York").yearFounded(1624).population(8804190).elevation(10).floodDanger("High").save();
            builder.state("New York").name("Albany").yearFounded(1614).population(97478).elevation(43).floodDanger("High").save();
            builder.state("New York").name("Buffalo").yearFounded(1810).population(261310).elevation(183).floodDanger("Medium").save();
            builder.state("Alberta").name("Edmonton").yearFounded(1795).population(932546).elevation(645).floodDanger("Medium").save();
            builder.state("Alberta").name("Calgary").yearFounded(1875).population(1239220).elevation(1045).floodDanger("Low").save();
            builder.state("Alberta").name("Red Deer").yearFounded(1913).population(99718).elevation(855).floodDanger("Low").save();

            response.put("insertedData", true);
            response.put("citiesInserted", builder.count());

        } catch (Exception exc) {
            return error(response, exc);
        }

        return ok(response);
    }

    private List<City> getCities(String state) {
        // Set up an alias for the partition key name in case it's a reserved word
        Map<String, String> attrNameAlias = Map.of("#state", "state");

        // Set up mapping of the partition name with the value
        Map<String, AttributeValue> attrValues = Map.of(":state", new AttributeValue().withS(state));

        DynamoDBQueryExpression<City> queryExpression = new DynamoDBQueryExpression<>();
        queryExpression.withKeyConditionExpression("#state = :state");
        queryExpression.setExpressionAttributeNames(attrNameAlias);
        queryExpression.setExpressionAttributeValues(attrValues);
        return this.mapper.query(City.class, queryExpression);
    }

    private CityBuilder createCityBuilder() {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withRegion(Regions.DEFAULT_REGION)
                .build();

        //Overriding the City DynamoDB table name (See JavaCdkStack)
        String tableName = System.getenv("CITYDYNAMODBTABLE");
        DynamoDBMapperConfig mapperConfig = DynamoDBMapperConfig.builder()
                .withTableNameOverride(TableNameOverride.withTableNameReplacement(tableName))
                .build();

        //Use of the Partition Index (state) and the sort key(city) for querying data
        //Let's just see if there is any data (for Alberta in this case)
        //We are only going to add data once, so if there is data we will not do again
        return new CityBuilder(new DynamoDBMapper(client, mapperConfig), mapperConfig);
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