FROM python:3
WORKDIR /install
COPY requirements.txt .
RUN pip install --install-option="--prefix=/install" -r requirements.txt


FROM python:3
COPY --from=0 /install /usr/local
WORKDIR /app
COPY src .
CMD ["python", "__init__.py"]