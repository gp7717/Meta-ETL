import logging
import azure.functions as func
from MetaETL import main

def main_function(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('MetaETL Azure Function triggered.')
    try:
        main()
        return func.HttpResponse("MetaETL ETL completed successfully.", status_code=200)
    except Exception as e:
        logging.error(f"MetaETL ETL failed: {e}")
        return func.HttpResponse(f"MetaETL ETL failed: {e}", status_code=500) 