package com.framgia.sqsreceiver.task;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

public class ReceiverTask implements Runnable {
    private static final Logger logger = Logger.getLogger(ReceiverTask.class.getSimpleName());
    @Override
    public void run() {
        final AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();
        final String queueUrl = sqs.getQueueUrl("DemoSQS").getQueueUrl();
        final SetQueueAttributesRequest setQueueAttributesRequest = new SetQueueAttributesRequest()
                .withQueueUrl(queueUrl)
                .addAttributesEntry("ReceiveMessageWaitTimeSeconds", "20");
        sqs.setQueueAttributes(setQueueAttributesRequest);
        final ReceiveMessageRequest receiveRequest = new ReceiveMessageRequest()
                .withQueueUrl(queueUrl)
                .withWaitTimeSeconds(20);
        while (true) {
            logger.info("#Start loop");
            List<Message> listMessage = sqs.receiveMessage(receiveRequest).getMessages();
            for (Message message : listMessage){
                List<String> values = new ArrayList<>(message.getAttributes().values());
                logger.info("#Receive: "+Arrays.toString(values.toArray()));
            }
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
            }
        }
    }
}
