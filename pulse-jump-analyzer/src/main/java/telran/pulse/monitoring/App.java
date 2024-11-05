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
import static telran.pulse.monitoring.Constants.*;




public class App {
	// FIXME move to DynamoDB Table
	HashMap<String, Integer> lastValues = new HashMap<>();
	static Logger logger = Logger.getLogger("pulse-value-analyzer");
	static {
		loggerSetUp();
		
	}
	
	public void handleRequest(DynamodbEvent event, Context context) {
		event.getRecords().forEach(r -> {

			Map<String, AttributeValue> map = r.getDynamodb().getNewImage();
			if (map == null) {
				System.out.println("No new image found");
			} else if (r.getEventName().equals("INSERT")) {
				String patientId = map.get("patientId").getN();
				Integer currentValue = Integer.parseInt(map.get("value").getN());
				String timestamp = map.get("timestamp").getN();
				Integer lastValue = lastValues.computeIfAbsent(patientId, k -> currentValue);
				if (isJump(currentValue, lastValue)) {
					jumpProcessing(patientId, currentValue, lastValue, timestamp);
				}
				lastValues.put(patientId, lastValue);

			} else {
				System.out.println(r.getEventName());
			}

		});
	}

	private void jumpProcessing(String patientId, Integer currentValue, Integer lastValue, String timestamp) {
		// FIXME move to DynamoDB Table
		System.out.printf("Jump: patientId is %s,lastValue is %d, currentValue is %d, timestamp is %s\n",
				patientId, lastValue, currentValue, timestamp);

	}

	private boolean isJump(Integer currentValue, Integer lastValue) {
		// FIXME
		float factor = 0.2f;
		return (float) Math.abs(currentValue - lastValue) / lastValue > factor;
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
