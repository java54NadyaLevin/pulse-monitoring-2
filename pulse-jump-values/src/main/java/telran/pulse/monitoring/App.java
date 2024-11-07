package telran.pulse.monitoring;

import java.util.HashMap;
import java.util.Map;
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
import static telran.pulse.monitoring.Constants.CURRENT_VALUE_ATTRIBUTE;
import static telran.pulse.monitoring.Constants.DEFAULT_FACTOR;
import static telran.pulse.monitoring.Constants.DEFAULT_LOGGER_LEVEL;
import static telran.pulse.monitoring.Constants.FACTOR_ENV_VARIABLE;
import static telran.pulse.monitoring.Constants.JUMP_VALUES_TABLE_NAME;
import static telran.pulse.monitoring.Constants.LOGGER_LEVEL_ENV_VARIABLE;
import static telran.pulse.monitoring.Constants.PATIENT_ID_ATTRIBUTE;
import static telran.pulse.monitoring.Constants.PREVIOUS_VALUE_ATTRIBUTE;
import static telran.pulse.monitoring.Constants.TIMESTAMP_ATTRIBUTE;
import static telran.pulse.monitoring.Constants.VALUE_ATTRIBUTE;

public class App {
	static DynamoDbClient client = DynamoDbClient.builder().build();
	static Builder request;
	static Logger logger = Logger.getLogger("pulse-jump-values");
	static {
		loggerSetUp();
		
	}
	
	public void handleRequest(DynamodbEvent event, Context context) {
		request = PutItemRequest.builder().tableName(JUMP_VALUES_TABLE_NAME);
		event.getRecords().forEach(r -> {
			Map<String, AttributeValue> newMap = r.getDynamodb().getNewImage();
			Map<String, AttributeValue> oldMap = r.getDynamodb().getOldImage();
			if (newMap == null || oldMap == null) {
				logger.warning("No dynamoDB image was found");
			} else if (r.getEventName().equals("MODIFY")) {
				int currentValue = Integer.parseInt(newMap.get(VALUE_ATTRIBUTE).getN());
				int lastValue = Integer.parseInt(oldMap.get(VALUE_ATTRIBUTE).getN());
				if(isJump(currentValue, lastValue)){
				client.putItem(request.item(getPutItemMap(newMap, oldMap)).build());
				logger.finer("PatientID: " + newMap.get(PATIENT_ID_ATTRIBUTE).getN() 
				+ "currentValue: " + newMap.get(VALUE_ATTRIBUTE).getN()
				+ "previousValue: " + oldMap.get(VALUE_ATTRIBUTE).getN()
				+ "timestamp: " + oldMap.get(TIMESTAMP_ATTRIBUTE).getN());
				}
			} else {
				logger.info(r.getEventName());
			}
		});
	}

	private Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> getPutItemMap(
			Map<String, AttributeValue> newMap, Map<String, AttributeValue> oldMap) {
		Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> res = new HashMap<>();
		res.put(PATIENT_ID_ATTRIBUTE, software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder()
				.n(newMap.get(PATIENT_ID_ATTRIBUTE).getN()).build());
		res.put(CURRENT_VALUE_ATTRIBUTE, software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder()
				.n(newMap.get(VALUE_ATTRIBUTE).getN()).build());
		res.put(PREVIOUS_VALUE_ATTRIBUTE, software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder()
				.n(oldMap.get(VALUE_ATTRIBUTE).getN()).build());
		res.put(TIMESTAMP_ATTRIBUTE, software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder()
				.n(newMap.get(TIMESTAMP_ATTRIBUTE).getN()).build());
		return res;
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
			res = Float.parseFloat(DEFAULT_FACTOR);
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
