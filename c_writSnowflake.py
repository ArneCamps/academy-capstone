from a_ETL_Weather import readandtransformweatherdata
from b_retrieveSnowFlakecredentials import get_snowflake_creds_from_sm

s3_path = "s3a://dataminded-academy-capstone-resources/raw/open_aq/*"
snowflake_credentials = 'snowflake/capstone/login'

sparkDF = readandtransformweatherdata(s3_path)

sfOptions = get_snowflake_creds_from_sm(snowflake_credentials)


(sparkDF.write
    .format(SNOWFLAKE_SOURCE_NAME)
    .options(sfOptions)
    .option("dbtable", "t2")
    .mode(SaveMode.Overwrite)
    .save()
)
