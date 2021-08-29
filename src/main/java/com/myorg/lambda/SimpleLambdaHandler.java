package com.myorg.lambda;

import org.json.simple.JSONObject;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.Arrays;

public class SimpleLambdaHandler implements RequestHandler<Object, Object>{
	Gson gson = new GsonBuilder().setPrettyPrinting().create();
	
  @SuppressWarnings("unchecked")
  @Override
  public Object handleRequest(Object event, Context context)
  {	  
	  JSONObject response = new JSONObject();
	  try
	  {

		//Custom environment variables created in JavaCdkStack
		JSONObject envObject = new JSONObject();		  		  
		envObject.put("REGION", System.getenv("REGION"));
		envObject.put("ENVIRONMENT", System.getenv("ENVIRONMENT"));
		envObject.put("S3BUCKET", System.getenv("S3BUCKET"));
		envObject.put("CITYDYNAMODBTABLE", System.getenv("CITYDYNAMODBTABLE"));
		response.put("customEnvironmentVariables", envObject);

		//All Environment variables
		response.put("environmentVariables", gson.toJson(System.getenv()));

		//Context
		response.put("context", gson.toJson(context));
		
		//We can see the request body here
		response.put("event", gson.toJson(event));
	  }
	  catch (Exception exc)
	  {
		  return error(response, exc);
	  }

	  return ok(response);
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