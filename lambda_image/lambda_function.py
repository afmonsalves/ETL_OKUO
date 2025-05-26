import main 

def handler(event, context):
    """
    Lambda handler for running the ETL.
    """
    try:
        main()
        return {
        "message": "ETL Completed"        }
    except Exception as e:
        return {
            "error": str(e),
            "message": "ETL Failed"
        }