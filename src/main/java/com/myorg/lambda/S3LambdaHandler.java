package com.myorg.lambda;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;


public class S3LambdaHandler implements RequestHandler<Object, Object> {

    @SuppressWarnings("unchecked")
    @Override
    public Object handleRequest(Object event, Context context) {
        JSONObject response = new JSONObject();
        try {

            final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.DEFAULT_REGION).build();
            ListObjectsV2Result result = s3.listObjectsV2(System.getenv("S3BUCKET"));
            List<S3ObjectSummary> objects = result.getObjectSummaries();

            ArrayList<String> files = new ArrayList<>();

            int numberOfBucketItems = 0;
            for (S3ObjectSummary os : objects) {
                files.add(String.format("Object[%d]:%s %s", numberOfBucketItems+=1, os.getKey(), os.getLastModified()));
            }

            response.put("bucket", System.getenv("S3BUCKET"));
            response.put("numberBucketItems", "" + numberOfBucketItems);
            JSONArray jsonArray = new JSONArray();
            jsonArray.addAll(files);
            response.put("objectList", jsonArray);

        } catch (Throwable exc) {
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

    private APIGatewayProxyResponseEvent error(JSONObject response, Throwable exc) {
        String exceptionString = String.format("error: %s: %s", exc.getMessage(), Arrays.toString(exc.getStackTrace()));
        response.put("Exception", exceptionString);

        return new APIGatewayProxyResponseEvent()
                .withStatusCode(500)
                .withBody(response.toJSONString())
                .withIsBase64Encoded(false);
    }

}