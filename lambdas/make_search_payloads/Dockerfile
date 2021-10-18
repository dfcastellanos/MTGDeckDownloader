FROM public.ecr.aws/lambda/python:3.8

COPY ./src/ ./requirements.txt ./

RUN python3.8 -m pip install -r requirements.txt -t .

CMD ["lambda_handlers.make_search_payloads_handler"]
