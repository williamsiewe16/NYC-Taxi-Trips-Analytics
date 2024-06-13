FROM quay.io/astronomer/astro-runtime:11.4.0

COPY aws_credentials .

COPY aws_config .

RUN mkdir -p ~/.aws && mv aws_credentials ~/.aws/credentials && mv aws_config ~/.aws/config

RUN pip install -r requirements.txt

# install dbt into a virtual environment
#RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
#    pip install --no-cache-dir dbt-snowflake==1.5.3 && deactivate

#RUN python -m venv py_venv && source py_venv/bin/activate && \
#pip install --no-cache-dir requests==2.32.3 boto3==1.34.115 && deactivate

