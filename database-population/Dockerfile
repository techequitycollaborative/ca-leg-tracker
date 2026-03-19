FROM python:3.12-slim

WORKDIR /pipeline

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright's Chromium browser + its system dependencies
RUN playwright install chromium
RUN playwright install-deps chromium

COPY . .

CMD ["python", "daily.py"]