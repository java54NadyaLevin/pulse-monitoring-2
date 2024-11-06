package telran.pulse.monitoring;

import java.util.*;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest.Builder;

import static telran.pulse.monitoring.Constants.*;


public class App {
	static DynamoDbClient client = DynamoDbClient.builder().build();
	static Builder request;
	static Logger logger = Logger.getLogger("pulse-jump-analyzer");
	static {
		loggerSetUp();
		
	}
	
	public void handleRequest(DynamodbEvent event, Context context) {
		request = PutItemRequest.builder().tableName(LAST_VALUES_TABLE_NAME);
		event.getRecords().forEach(r -> {
			Map<String, AttributeValue> map = r.getDynamodb().getNewImage();
			if (map == null) {
				System.out.println("No new image found");
			} else if (r.getEventName().equals("INSERT")) {
				// String patientId = map.get("patientId").getN();
				// Integer currentValue = Integer.parseInt(map.get("value").getN());
				// String timestamp = map.get("timestamp").getN();
				client.putItem(request.item(getPutItemMap(map)).build());
			} else {
				System.out.println(r.getEventName());
			}

		});
	}

	private Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> getPutItemMap(
			Map<String, AttributeValue> map) {
		Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> res = new HashMap<>();
		res.put(PATIENT_ID_ATTRIBUTE, software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder()
				.n(map.get(PATIENT_ID_ATTRIBUTE).getN()).build());
		res.put(VALUE_ATTRIBUTE, software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder()
				.n(map.get(VALUE_ATTRIBUTE).getN()).build());
		return res;
	}
	private void jumpProcessing(String patientId, Integer currentValue, Integer lastValue, String timestamp) {
		// FIXME move to DynamoDB Table
		System.out.printf("Jump: patientId is %s,lastValue is %d, currentValue is %d, timestamp is %s\n",
				patientId, lastValue, currentValue, timestamp);

	}

	private boolean isJump(Integer currentValue, Integer lastValue) {
		float factor = getFloatFactor();
		return (float) Math.abs(currentValue - lastValue) / lastValue > factor;
	}

	private static float getFloatFactor() {
		String factor = System.getenv()
		.getOrDefault(FACTOR_ENV_VARIABLE, DEFAULT_FACTOR);
		float res = 1;
		try {
			res = Float.parseFloat(factor);
		} catch (Exception e) {
			res = Float.parseFloat(FACTOR);
		}
		return res;
	}
		private static void loggerSetUp() {
		Level loggerLevel = getLoggerLevel();
		LogManager.getLogManager().reset();
		Handler handler = new ConsoleHandler();
		logger.setLevel(loggerLevel);
		handler.setLevel(Level.FINEST);
		logger.addHandler(handler);
	}


	private static Level getLoggerLevel() {
		String levelStr = System.getenv()
		.getOrDefault(LOGGER_LEVEL_ENV_VARIABLE, DEFAULT_LOGGER_LEVEL);
		Level res = null;
		try {
			res = Level.parse(levelStr);
		} catch (Exception e) {
			res = Level.parse(DEFAULT_LOGGER_LEVEL);
		}
		return res;
	}
}
