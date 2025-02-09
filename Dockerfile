FROM python:3.11-slim

WORKDIR /app

# Copy the requirements file to the container
COPY requirements.txt requirements.txt

# Copy environment variables
COPY .env .env

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code to the container
COPY ./app .

EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

ENTRYPOINT ["streamlit", "run", "Home.py"]