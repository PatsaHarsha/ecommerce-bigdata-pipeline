import os
import subprocess
import time
import re
from PIL import Image, ImageDraw, ImageFont
from playwright.sync_api import sync_playwright

os.makedirs('resultsmile1', exist_ok=True)

def text_to_image(text, filename):
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    text = ansi_escape.sub('', text)
    lines = text.split('\n')
    width = 1200
    height = max(400, len(lines) * 20 + 40)
    img = Image.new('RGB', (width, height), color='black')
    d = ImageDraw.Draw(img)
    try:
        font = ImageFont.truetype("consola.ttf", 14)
    except:
        font = ImageFont.load_default()
    y_text = 20
    for line in lines:
        d.text((20, y_text), line, font=font, fill=(0, 255, 0))
        y_text += 20
    img.save(f'resultsmile1/{filename}')
    print(f"Saved {filename}")

print("Starting generation of screenshots...")

# 1. Architecture
res = subprocess.run(['docker', 'compose', 'ps'], capture_output=True, text=True)
text_to_image("docker compose ps\n\n" + res.stdout, "1_architecture.png")

# 2. Ingestion
print("Running producer for 12 seconds to capture output...")
env = os.environ.copy()
env['PYTHONIOENCODING'] = 'utf-8'
proc = subprocess.Popen(['python', 'producer.py'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)
time.sleep(12)
proc.terminate()
out, _ = proc.communicate()
text_to_image("python producer.py\n\n" + out, "2_ingestion.png")

# Start streaming job in background to capture active UI and monitoring
print("Starting streaming job in background...")
stream_proc = subprocess.Popen(['docker', 'compose', 'exec', 'jupyter', 'spark-submit', '/home/jovyan/work/streaming_job.py'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
time.sleep(20) # let it initialize

# 3 & 5. Playwright
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    
    # 3. Spark UI
    print("Capturing Spark UI...")
    try:
        page.goto('http://localhost:8080', timeout=10000)
        time.sleep(2)
        page.screenshot(path='resultsmile1/3_storage_spark.png')
        print("Saved 3_storage_spark.png")
    except Exception as e:
        print("Error capturing Spark UI:", e)
        text_to_image(f"Error capturing Spark UI:\n{e}", "3_storage_spark.png")

    # 5. MinIO Console
    print("Capturing MinIO Console...")
    try:
        page.goto('http://localhost:9001', timeout=10000)
        page.fill('input[name="accessKey"]', 'admin')
        page.fill('input[name="secretKey"]', 'bigdata123')
        page.click('button[type="submit"]')
        time.sleep(3)
        page.goto('http://localhost:9001/browser/warehouse/events', timeout=10000)
        time.sleep(3)
        page.screenshot(path='resultsmile1/5_minio_console.png')
        print("Saved 5_minio_console.png")
    except Exception as e:
        print("Error capturing MinIO:", e)
        text_to_image(f"Error capturing MinIO:\n{e}", "5_minio_console.png")
        
    browser.close()

# 4. Streaming Proof
print("Capturing streaming proof...")

monitor_script = """
import time
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Monitor").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
for i in range(4):
    try:
        count = spark.read.format("delta").load("s3a://warehouse/events").count()
        print(f"[{time.strftime('%H:%M:%S')}] Active Stream writing... Total records in Delta Lake: {count}")
    except:
        pass
    time.sleep(3)
"""
with open('notebooks/monitor.py', 'w') as f:
    f.write(monitor_script)

# Make sure producer is running so count increases
prod_proc = subprocess.Popen(['python', 'producer.py'], stdout=subprocess.DEVNULL, env=env)
mon_res = subprocess.run(['docker', 'compose', 'exec', 'jupyter', 'python', '/home/jovyan/work/monitor.py'], capture_output=True, text=True)
prod_proc.terminate()

text_to_image("Monitoring streaming output...\n\n" + mon_res.stdout, "4_streaming_proof.png")

# Terminate stream process
try:
    stream_proc.terminate()
except:
    pass

print("Done. All files saved to resultsmile1 folder.")
