# Use the official Python 3.11 image as a parent image
FROM python:3.11

# Set the working directory in the container to /app
WORKDIR /app

# Copy only the requirements.txt first, to leverage Docker cache
COPY requirements.txt /app/

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the current directory contents into the container at /app
COPY . /app

# Run app.py when the container launches
CMD ["python", "src/app.py"]
