FROM public.ecr.aws/lambda/python:3.12

COPY requirements.txt ${LAMBDA_TASK_ROOT}

RUN pip install -r requirements.txt

COPY config.py ${LAMBDA_TASK_ROOT}/

COPY s3_client.py ${LAMBDA_TASK_ROOT}/

COPY main.py ${LAMBDA_TASK_ROOT}/

COPY .env ${LAMBDA_TASK_ROOT}/

COPY lambda_function.py ${LAMBDA_TASK_ROOT}/

CMD ["lambda_function.handler"]