package telran.pulse.monitoring;

public interface Constants {
    String PATIENT_ID_ATTRIBUTE = "patientId";
    String TIMESTAMP_ATTRIBUTE = "timestamp";
    String PREVIOUS_VALUE_ATTRIBUTE = "previousValue";
    String CURRENT_VALUE_ATTRIBUTE = "currentValue";
    String VALUE_ATTRIBUTE = "value";
    int MIN_THRESHOLD_PULSE_VALUE = 50;
    int MAX_THRESHOLD_PULSE_VALUE = 190;
    String ABNORMAL_VALUES_TABLE_NAME = "pulse_abnormal_values";
    String LAST_VALUES_TABLE_NAME = "pulse_last_value";
    String JUMP_VALUES_TABLE_NAME = "pules_jump_values";
    String LOGGER_LEVEL_ENV_VARIABLE = "LOGGER_LEVEL";
    String DEFAULT_LOGGER_LEVEL = "INFO";
    String FACTOR_ENV_VARIABLE = "FACTOR";
    String DEFAULT_FACTOR = "0.2";

}
