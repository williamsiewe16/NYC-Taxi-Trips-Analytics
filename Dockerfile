FROM quay.io/astronomer/astro-runtime:11.4.0

# install dbt into a virtual environment
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake==1.5.3 && deactivate

RUN python -m venv py_venv && source py_venv/bin/activate && \
pip install --no-cache-dir requests==2.32.3 google-cloud-storage==2.16.0 && deactivate