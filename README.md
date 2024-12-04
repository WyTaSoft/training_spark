# Spark-Scala Application in Docker

---

## Prerequisites
1. Docker must be installed and running on your system.
2. Ensure your Spark application JAR is correctly placed and referenced in the Dockerfile.
3. Adjust paths in the commands to match your local environment.

---

## Build and Run Instructions

### Step 1: Build the Docker Image
Use the `docker build` command to create a Docker image:
```bash
docker build -t spark-scala-app .
```
-t spark-scala-app: Tags the Docker image with the name spark-scala-app.
.: Specifies the current directory containing the Dockerfile.

### Step 2: Run the Spark Application
To execute your Spark-Scala application, run the following command:

```bash
docker run --rm -v C:/Users/mehdi/mtajmouati/Training/data/project/datalake:/prd/project/datalake -p 4040:4040 -p 8080:8080 spark-scala-app
```

Explanation
--rm: Automatically removes the container after it stops.
-v C:/Users/mehdi/mtajmouati/Training/data/project/datalake:/prd/project/datalake: Mounts your local directory to /prd/project/datalake inside the container.
-p 4040:4040: Maps the Spark Web UI port to monitor jobs.
-p 8080:8080: Maps another port, typically for additional services.
spark-scala-app: Refers to the Docker image built in Step 1.

### Step 3: Debugging or Interactive Shell
To manually debug or interact with the container, use this command:

```bash
docker run -it --entrypoint /bin/bash -v C:/Users/mehdi/mtajmouati/Training/data/project/datalake:/prd/project/datalake spark-scala-app
```

### Explanation
-it: Starts an interactive session with the container.
--entrypoint /bin/bash: Overrides the default entry point to provide access to the Bash shell.

### Accessing the Spark Web UI
URL: Open http://localhost:4040 in your browser to view and monitor Spark jobs.