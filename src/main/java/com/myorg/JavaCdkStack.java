package com.myorg;

import java.util.Random;
import com.amazonaws.regions.Regions;
import java.util.HashMap;
import software.amazon.awscdk.core.CfnOutput;
import software.amazon.awscdk.core.Construct;
import software.amazon.awscdk.core.Duration;
import software.amazon.awscdk.core.RemovalPolicy;
import software.amazon.awscdk.core.Stack;
import software.amazon.awscdk.core.StackProps;
import software.amazon.awscdk.services.lambda.Function;
import software.amazon.awscdk.services.lambda.Runtime;
import software.amazon.awscdk.services.lambda.Code;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.dynamodb.AttributeType;
import software.amazon.awscdk.services.dynamodb.GlobalSecondaryIndexProps;
import software.amazon.awscdk.services.dynamodb.LocalSecondaryIndexProps;
import software.amazon.awscdk.services.dynamodb.Table;
import software.amazon.awscdk.services.dynamodb.TableProps;
import software.amazon.awscdk.services.apigateway.LambdaIntegration;
import software.amazon.awscdk.services.apigateway.Method;
import software.amazon.awscdk.services.apigateway.Resource;
import software.amazon.awscdk.services.apigateway.RestApi;
import software.amazon.awscdk.services.dynamodb.Attribute;

public class JavaCdkStack extends Stack {
    public JavaCdkStack(final Construct scope, final String id) {
        this(scope, id, null);
    }

    public JavaCdkStack(final Construct scope, final String id, final StackProps props) {
        super(scope, id, props);

        // Environment variable to separate the environments
        String environment = "dev";

        //Create an S3 Bucket
        Random rand = new Random();
        int randNumber = rand.nextInt(100000);
        String bucketName = environment+ "-mys3bucket-" + randNumber;

        //Remember you must empty the s3 bucket before you can actually delete/restore it
        Bucket bucket = Bucket.Builder.create(this, "mys3bucket")
        .bucketName(bucketName)
        .removalPolicy(RemovalPolicy.DESTROY)
        .build();

        String tableName = environment + "-"+ "City";
        //Create a DynamoDB Table
        TableProps tableProps = TableProps.builder()
        	    .partitionKey(Attribute.builder()
        	         .name("state")
        	         .type(AttributeType.STRING)
        	         .build())
                .sortKey(Attribute.builder()
           	    	 .name("city")
           	    	 .type(AttributeType.STRING)
           	    	 .build())        	    
                .readCapacity(1)
        	    .writeCapacity(1)
        	    .removalPolicy(RemovalPolicy.DESTROY)
        	    .tableName(tableName)
        	    .build();
        Table cityDynamoDbTable = new Table(this, tableName, tableProps);
        
        //Local Secondary Index
        cityDynamoDbTable.addLocalSecondaryIndex(LocalSecondaryIndexProps.builder()
        		.indexName("FoundedLSIPopulationIndex")
        		.sortKey(Attribute.builder()
           	    	 .name("founded")
           	    	 .type(AttributeType.NUMBER)
           	    	 .build())
        		.build());

        //Global Secondary Index
        cityDynamoDbTable.addGlobalSecondaryIndex(GlobalSecondaryIndexProps.builder()
        		.indexName("FloodDangerGSIIndex")
        		.partitionKey(Attribute.builder()
           	    	 .name("floodDanger")
           	    	 .type(AttributeType.STRING)
           	    	 .build())
        		.sortKey(Attribute.builder()
              	    	 .name("elevationMetres")
              	    	 .type(AttributeType.NUMBER)
              	    	 .build())
        		.readCapacity(1)
        		.writeCapacity(1)
        		.build());
  

        //Lambda Environment Variables to pass to the Lambdas
        HashMap<String, String> env = new HashMap <String, String>();
        env.put("REGION", ""+Regions.US_WEST_2);
        env.put("ENVIRONMENT", environment);
        env.put("S3BUCKET", bucketName);
        env.put("CITYDYNAMODBTABLE", cityDynamoDbTable.getTableName());
        
        //Lambda setup
        Function simpleLambdaFunction = Function.Builder.create(this, "simpleLambda")
                .runtime(Runtime.JAVA_11)
                .functionName("simpleLambda")
                .timeout(Duration.seconds(50))
                .memorySize(512)
                .environment(env)
                .code(Code.fromAsset("target/java_cdk-0.1.jar"))
                .handler("com.myorg.lambda.SimpleLambdaHandler::handleRequest")
                .build();
        
        //Sometimes lambdas might timeout if they don't have enough memory
        int memorySize = 1024;
        Function s3LambdaFunction = Function.Builder.create(this, "s3Lambda")
                .runtime(Runtime.JAVA_11)
                .functionName("s3Lambda")
                .timeout(Duration.seconds(20))
                .memorySize(memorySize)
                .environment(env)
                .code(Code.fromAsset("target/java_cdk-0.1.jar"))
                .handler("com.myorg.lambda.S3LambdaHandler::handleRequest")
                .build();
        bucket.grantReadWrite(s3LambdaFunction);
     
        Function insertDynamoDbLambdaFunction = Function.Builder.create(this, "insertDynamoDbLambda")
                .runtime(Runtime.JAVA_11)
                .functionName("insertDynamoDbLambda")
                .timeout(Duration.seconds(20))
                .memorySize(memorySize)
                .environment(env)
                .code(Code.fromAsset("target/java_cdk-0.1.jar"))
                .handler("com.myorg.lambda.InsertDynamoDbLambdaHandler::handleRequest")
                .build();
        cityDynamoDbTable.grantFullAccess(insertDynamoDbLambdaFunction);

        //Use of Scan Lambda
        Function scanLambdaFunction = Function.Builder.create(this, "scanLambda")
                .runtime(Runtime.JAVA_11)
                .functionName("scanLambda")
                .timeout(Duration.seconds(20))
                .memorySize(memorySize)
                .environment(env)
                .code(Code.fromAsset("target/java_cdk-0.1.jar"))
                .handler("com.myorg.lambda.ScanLambdaHandler::handleRequest")
                .build();
        cityDynamoDbTable.grantFullAccess(scanLambdaFunction);

        //Use of Partition search
        Function partitionLambdaFunction = Function.Builder.create(this, "partitionLambda")
                .runtime(Runtime.JAVA_11)
                .functionName("partitionLambda")
                .timeout(Duration.seconds(20))
                .memorySize(memorySize)
                .environment(env)
                .code(Code.fromAsset("target/java_cdk-0.1.jar"))
                .handler("com.myorg.lambda.PartitionLambdaHandler::handleRequest")
                .build();
        cityDynamoDbTable.grantFullAccess(partitionLambdaFunction);

        //Use of Global Secondary Index
        Function gsiLambdaFunction = Function.Builder.create(this, "gsiLambda")
                .runtime(Runtime.JAVA_11)
                .functionName("gsiLambda")
                .timeout(Duration.seconds(20))
                .memorySize(memorySize)
                .environment(env)
                .code(Code.fromAsset("target/java_cdk-0.1.jar"))
                .handler("com.myorg.lambda.GSILambdaHandler::handleRequest")
                .build();
        cityDynamoDbTable.grantFullAccess(gsiLambdaFunction);
        
        //Use of the Local Secondary Index
        Function lsiLambdaFunction = Function.Builder.create(this, "lsiLambda")
                .runtime(Runtime.JAVA_11)
                .functionName("lsiLambda")
                .timeout(Duration.seconds(20))
                .memorySize(memorySize)
                .environment(env)
                .code(Code.fromAsset("target/java_cdk-0.1.jar"))
                .handler("com.myorg.lambda.LSILambdaHandler::handleRequest")
                .build();
        cityDynamoDbTable.grantFullAccess(lsiLambdaFunction);

        
        //API Gateway Configuration (Allowing Lambdas to be called via the API Gateway
        RestApi api = RestApi.Builder.create(this, "Java CDK")
                .restApiName("Java CDK").description("Java CDK")
                .build();

        LambdaIntegration simpleIntegration = LambdaIntegration.Builder.create(simpleLambdaFunction) 
                .requestTemplates(new HashMap<String, String>() {{
                    put("application/json", "{ \"statusCode\": \"200\" }");
                }}).build();

        LambdaIntegration s3Integration = LambdaIntegration.Builder.create(s3LambdaFunction) 
                .requestTemplates(new HashMap<String, String>() {{
                    put("application/json", "{ \"statusCode\": \"200\" }");
                }}).build();

        LambdaIntegration insertDynamoDbIntegration = LambdaIntegration.Builder.create(insertDynamoDbLambdaFunction) 
                .requestTemplates(new HashMap<String, String>() {{
                    put("application/json", "{ \"statusCode\": \"200\" }");
                }}).build();
        
        LambdaIntegration scanIntegration = LambdaIntegration.Builder.create(scanLambdaFunction) 
                .requestTemplates(new HashMap<String, String>() {{
                    put("application/json", "{ \"statusCode\": \"200\" }");
                }}).build();        

        LambdaIntegration partitionIntegration = LambdaIntegration.Builder.create(partitionLambdaFunction) 
                .requestTemplates(new HashMap<String, String>() {{
                    put("application/json", "{ \"statusCode\": \"200\" }");
                }}).build();
        
        LambdaIntegration gsiIntegration = LambdaIntegration.Builder.create(gsiLambdaFunction) 
                .requestTemplates(new HashMap<String, String>() {{
                    put("application/json", "{ \"statusCode\": \"200\" }");
                }}).build();   
        LambdaIntegration lsiIntegration = LambdaIntegration.Builder.create(lsiLambdaFunction) 
                .requestTemplates(new HashMap<String, String>() {{
                    put("application/json", "{ \"statusCode\": \"200\" }");
                }}).build();           
        
        //It is up to you if you want to structure your lambdas in separate APIGateway APIs (RestApi)

        //Option 1: Adding at the top level of the APIGateway API
        //api.getRoot().addMethod("POST", helloIntegration);

        //Option 2: Or break out resources under one APIGateway API as follows      
        Resource helloResource = api.getRoot().addResource("simple");
        Method simpleMethod = helloResource.addMethod("POST", simpleIntegration);
        Resource s3Resource = api.getRoot().addResource("s3");
        Method s3Method = s3Resource.addMethod("POST", s3Integration);
        Resource insertDynamoDbResource = api.getRoot().addResource("insertDynamoDb");
        Method insertDynamoDbMethod = insertDynamoDbResource.addMethod("POST", insertDynamoDbIntegration);      
        Resource scanResource = api.getRoot().addResource("scanDynamoDb");
        Method scanMethod = scanResource.addMethod("POST", scanIntegration);      
        Resource partitionResource = api.getRoot().addResource("partitionDynamoDb");
        Method partitionMethod = partitionResource.addMethod("POST", partitionIntegration);   
        Resource gsiResource = api.getRoot().addResource("gsiDynamodb");
        Method gsiMethod = gsiResource.addMethod("POST", gsiIntegration);      
        Resource lsiResource = api.getRoot().addResource("lsiDynamodb");
        Method lsiMethod = lsiResource.addMethod("POST", lsiIntegration);      
        

       //CDK Output to display to the user the resultant information and urls
       CfnOutput.Builder.create(this, "ZA RegionOutput")
       .description("")
        .value("Region:"+ this.getRegion())
        .build();
       
       CfnOutput.Builder.create(this, "ZB S3 Bucket")
                    .description("")
                    .value("S3 Bucket:"+ bucket.getBucketName())
                    .build();
       
       CfnOutput.Builder.create(this, "ZC DynamoDB Table")
       .description("")
       .value("DynamoDBTable:"+ cityDynamoDbTable.getTableName())
       .build();
       
       String urlPrefix = api.getUrl().substring(0, api.getUrl().length()-1);
  
      CfnOutput.Builder.create(this, "ZD Simple Lambda")
       .description("")
       .value("Simple Lambda:"+urlPrefix + simpleMethod.getResource().getPath())
       .build();
      
      CfnOutput.Builder.create(this, "ZE S3 Lambda")
      .description("")
      .value("S3 Lambda:"+urlPrefix + s3Method.getResource().getPath())
      .build();

      CfnOutput.Builder.create(this, "ZF Insert DynamoDb Lambda")
      .description("")
      .value("Insert DynamoDb Lambda:"+urlPrefix + insertDynamoDbMethod.getResource().getPath())
      .build();
      
      CfnOutput.Builder.create(this, "ZG Scan DynamoDb Lambda")
      .description("")
      .value("Scan DynamoDb Lambda:"+urlPrefix + scanMethod.getResource().getPath())
      .build();

      CfnOutput.Builder.create(this, "ZH Partition DynamoDb Lambda")
      .description("")
      .value("Partition DynamoDb Lambda:"+urlPrefix + partitionMethod.getResource().getPath())
      .build();
      
      CfnOutput.Builder.create(this, "ZI GSI DynamoDb Lambda")
      .description("")
      .value("GSI DynamoDb Lambda:"+urlPrefix + gsiMethod.getResource().getPath())
      .build();

      CfnOutput.Builder.create(this, "ZJ LSI DynamoDb Lambda")
      .description("")
      .value("LSI DynamoDb Lambda:"+urlPrefix + lsiMethod.getResource().getPath())
      .build();
      
    }
}