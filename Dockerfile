# Use an official AWS Lambda Python base image
ARG PYTHON_VERSION=3.11  # Or 3.9, 3.10, 3.12 as needed
FROM public.ecr.aws/lambda/python:${PYTHON_VERSION}

# Copy the requirements file into the image
# This path is relative to the build context (repos/zephyr/zephyr/)
COPY requirements.txt ${LAMBDA_TASK_ROOT}/

# Install dependencies using pip
# Using --no-cache-dir reduces image size
RUN pip install --no-cache-dir -r ${LAMBDA_TASK_ROOT}/requirements.txt

# Copy your application code into the image
# *** Use your actual script name here ***
# This path is also relative to the build context (repos/zephyr/zephyr/)
COPY backend.py ${LAMBDA_TASK_ROOT}/

# Set the CMD to your handler function
# *** Use your actual script name and the handler function name ***
CMD [ "backend.lambda_handler" ]