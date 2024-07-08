package org.example;

import java.io.IOException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.AmazonRekognitionException;
import com.amazonaws.services.rekognition.model.DetectLabelsRequest;
import com.amazonaws.services.rekognition.model.DetectLabelsResult;
import com.amazonaws.services.rekognition.model.Image;
import com.amazonaws.services.rekognition.model.Label;
import com.amazonaws.services.rekognition.model.S3Object;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.springframework.boot.SpringApplication;

public class AWSObjectRekognitionClass {
    public static void main(String[] args) throws IOException, InterruptedException {
        SpringApplication.run(AWSObjectRekognitionClass.class, args);

        Regions clientRegion = Regions.US_EAST_1;
        String bucketName = "cs643-njit-project1"; //NJIT S3 bucket name

        try {
            //S3Client to communicate with S3 bucket
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(clientRegion)
                    .build();

            // Create an SQS client
            AmazonSQS sqsClient = AmazonSQSClientBuilder.standard()
                    .withRegion(clientRegion)
                    .build();

            // Create a FIFO queue named MyQueue.fifo, if it doesn't already exist
            if (!queueExists(sqsClient, "MyQueue.fifo")) {
                Map<String, String> attributes = new HashMap<>();
                attributes.put("FifoQueue", "true");
                attributes.put("ContentBasedDeduplication", "true");
                sqsClient.createQueue(new CreateQueueRequest().withQueueName("MyQueue.fifo").withAttributes(attributes));
            }

            System.out.println("Listing objects...");
            // maxKeys is set to 2 to demonstrate the use of
            // ListObjectsV2Result.getNextContinuationToken()
            ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName);
            ListObjectsV2Result result;
            do {
                result = s3Client.listObjectsV2(req);
                for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
                    //Iterates through each object (S3ObjectSummary) and fetches its key (photo), which represents the image file name.
                    String photo = objectSummary.getKey();
                    //Create a RekognitionClient instance for Rekognition operations.
                    AmazonRekognition rekognitionClient = AmazonRekognitionClientBuilder.defaultClient();
                    //DetectLabelsRequest is used to detect labels (objects) in images.
                    DetectLabelsRequest request = new DetectLabelsRequest()
                            .withImage(new Image().withS3Object(new S3Object().withName(photo).withBucket(bucketName)))
                            .withMaxLabels(10).withMinConfidence(75F);
                    try {
                        //detect the objects in images
                        DetectLabelsResult result1 = rekognitionClient.detectLabels(request);
                        List<Label> labels = result1.getLabels();

                        Hashtable<String, Integer> numbers = new Hashtable<>();
                        for (Label label : labels) {
                            // Check if the recognized label is a Car and if confidence>80, then push to SQS and message(image key) is sent to SQS
                            if (label.getName().equals("Car") && label.getConfidence() > 80) {
                                System.out.print("Detected labels for:  " + photo + " => ");
                                numbers.put(label.getName(), Math.round(label.getConfidence()));
                                System.out.print("Label: " + label.getName() + " ,");
                                System.out.print("Confidence: " + label.getConfidence().toString() + "\n");
                                System.out.println("Pushed to SQS.");

                                // Send message to SQS
                                SendMessageRequest sendMessageRequest = new SendMessageRequest()
                                        .withQueueUrl(getQueueUrl(sqsClient, "MyQueue.fifo"))
                                        .withMessageBody(photo)
                                        .withMessageGroupId("Default");
                                sqsClient.sendMessage(sendMessageRequest);
                            }
                        }

                    } catch (AmazonRekognitionException e) {
                        e.printStackTrace();
                    }
                }
                String token = result.getNextContinuationToken();
                // System.out.println("Next Continuation Token: " + token);
                req.setContinuationToken(token);
            } while (result.isTruncated());
        } catch (AmazonServiceException e) {
            e.printStackTrace();
        } catch (SdkClientException e) {
            e.printStackTrace();
        }
    }

    private static boolean queueExists(AmazonSQS sqsClient, String queueName) {
        return sqsClient.listQueues().getQueueUrls().stream()
                .anyMatch(url -> url.endsWith("/" + queueName));
    }

    private static String getQueueUrl(AmazonSQS sqsClient, String queueName) {
        return sqsClient.getQueueUrl(queueName).getQueueUrl();
    }
}
