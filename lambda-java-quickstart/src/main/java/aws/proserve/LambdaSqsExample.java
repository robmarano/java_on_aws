package aws.proserve;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.AmazonSQSException;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import java.util.Map;
import java.util.Date;
import java.util.List;

/**
 * Run a Java-based Lambda with SQS using an env var with a q name!
 *
 */
public class LambdaSqsExample implements RequestHandler<Object, Object> 
{
	public static String QUEUE_NAME = "QUEUE_NAME";

	protected AmazonSQS sqs;
	protected String queueUrl;

	@Override
	public Object handleRequest(Object input, Context context)
	{
        Map<String, String> env = System.getenv();
        String queueName = env.get(LambdaSqsExample.QUEUE_NAME);
    	sqs = AmazonSQSClientBuilder.defaultClient();
        System.out.println(
        	String.format("Example how to run a Lambda with SQS using an env var with q name:", queueName)
		);
        queueUrl = sqs.getQueueUrl(queueName).getQueueUrl();
        System.out.println(String.format("queue url = '%s'", queueUrl));

        // receive messages from the queue
        List<Message> messages = sqs.receiveMessage(queueUrl).getMessages();

        System.out.println(String.format("Printing messages in queue name '%s'", queueName));
        printMessages(messages);
        System.out.println(String.format("Deleting messages in queue name '%s'", queueName));
        deleteMessages(messages);

        String returnOutput = String.format("completed processing %s", input);
        return returnOutput;
    }

    public void printMessages(List<Message> messages)
    {
    	// print messages from the queue
    	for (Message m : messages) {
    	    System.out.println(m);
    	}
    }

    public void deleteMessages(List<Message> messages)
    {
    	// delete messages from the queue
    	for (Message m : messages) {
    	    sqs.deleteMessage(queueUrl, m.getReceiptHandle());
    	}
    }
}
