package com.syw.ors.common;

public interface QAConstants {
	//Setting up Big Query and PubSub PArameters
	public static final String PROJECT_ID_QA = "syw-ors-qa";
	public static final String TEMP_LOCATION_QA = "gs://ms_orslogs/temp";
	public static final String STAGING_LOCATION_QA = "gs://ms_orslogs/staging";
	public static final String PUBSUB_REPOSITORY_TOPIC = "repository-changes.default";
	public static final String DATASET_ID_QA = "ors_logs";
	public static final String ISSUANCE_TABLE_ID_QA = "ors_issuance_logs_master";
	public static final String ERROR_ISSUANCE_TABLE_ID_QA = "ors_issuance_error_log";
	public static final String SERVICE_URL_STR = "https://syw-ors-qa.appspot.com";
	
	
	public static final String ISSUANCE_TABLE_PATH = PROJECT_ID_QA + ":" + DATASET_ID_QA + "." + ISSUANCE_TABLE_ID_QA;
	public static final String ISSUANCE_ERROR_TABLE_PATH = PROJECT_ID_QA + ":" + DATASET_ID_QA + "." + ERROR_ISSUANCE_TABLE_ID_QA;
		
	public static final String PUBSUB_REPOSITORY_TOPIC_PATH = "projects" + "/" + PROJECT_ID_QA + "/" + "topics" + "/" + PUBSUB_REPOSITORY_TOPIC;
	
	public static final String RAW_DATA = "raw_data";
	public static final String ERROR_LOG = "error_log";
	
	
	public static final String LOAD_TIME = "load_time";
	public static final String DATA_JSON = "data_json";
	public static final String REQUEST_STATUS = "request_status";
	public static final String LOG_TYPE = "log_type";
	public static final String INSERT_ID = "insert_id";
	public static final String START_TIME = "start_time";
	public static final String VERSION_ID = "version_id";
	public static final String CURRENT_TIME = "current_time";
	
	public static final String ISSUE_TYPE = "issue_type";
	public static final String MEMBER_ID = "member_id";
	public static final String ISSUE_TIME = "issue_time";
	public static final String OFFER_CODE = "offer_code";
	public static final String OFFER_TYPE = "offer_type";
	public static final String OFFER_NAME = "offer_name";
	public static final String AMOUNT_TYPE = "amount_type";
	public static final String AMOUNT = "amount";
	public static final String SEC_AMOUNT_TYPE = "sec_amount_type";
	public static final String SEC_AMOUNT = "sec_amount";
	public static final String PRODUCT_GROUP = "product_group";
	public static final String PRODUCT_SUB_GROUP = "product_subgroup";
	public static final String NUMBER_OF_OFFERS = "number_of_offers";
	public static final String MAX_CATALINA_OFFERS = "max_catalina_offers";
}
