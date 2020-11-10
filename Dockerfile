FROM python:3.8-buster

RUN apt update \
    && apt install --no-install-recommends --no-install-suggests -y ffmpeg \
    && apt remove --purge --auto-remove -y && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV LD_LIBRARY_PATH=/opt/vc/lib
ENV PATH=$PATH:/opt/vc/bin

ENTRYPOINT ["python", "app.py"]