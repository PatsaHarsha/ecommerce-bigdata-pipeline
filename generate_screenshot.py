import subprocess
from PIL import Image, ImageDraw, ImageFont
import re

def create_terminal_screenshot():
    # Run docker-compose ps
    result = subprocess.run(['docker-compose', 'ps'], capture_output=True, text=True)
    text = result.stdout
    
    # Strip ANSI escape codes
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    text = ansi_escape.sub('', text)

    # Calculate image size
    lines = text.split('\n')
    width = 1200
    height = max(400, len(lines) * 20 + 40)

    # Create image with black background
    img = Image.new('RGB', (width, height), color='black')
    d = ImageDraw.Draw(img)
    
    # Try to load a monospaced font
    try:
        font = ImageFont.truetype("consola.ttf", 14)
    except:
        font = ImageFont.load_default()

    # Draw text
    y_text = 20
    for line in lines:
        d.text((20, y_text), line, font=font, fill=(0, 255, 0))
        y_text += 20

    img.save('task1_docker_ps.png')

if __name__ == '__main__':
    create_terminal_screenshot()
