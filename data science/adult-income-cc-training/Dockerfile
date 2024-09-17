FROM us-docker.pkg.dev/deeplearning-platform-release/gcr.io/tf2-cpu.2-13.py310

WORKDIR /app

COPY . .

RUN pip install -r requirements.txt

ENTRYPOINT ["python", "train.py"]