package com.syw.ors.pipeline.common;

public interface ProdConstants {

	//Setting up Big Query and PubSub PArameters
	public static final String PROJECT_ID_PROD = "syw-ors-1226";
	public static final String PUBSUB_TOPIC_TO_READ_PROD = "repository-changes.default";
	public static final String STAGING_LOCATION_PROD = "gs://ms_orslog/staging";
	public static final String TEMP_LOCATION_PROD = "gs://ms_orslog/temp";
	public static final String DATASET_ID_PROD = "ors_logs";
	public static final String REQUEST_TABLE_ID_PROD = "ors_client_requests_daily_partitioned";
	public static final String ISSUANCE_TABLE_ID_PROD = "ors_issuance_logs_daily_partitioned";
	
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
	public static final String PRODUCT_SUB_GROUP = "product_sub_group";
	public static final String NUMBER_OF_OFFERS = "number_of_offers";
	public static final String MAX_CATALINA_OFFERS = "max_catalina_offers";
	
	
}
