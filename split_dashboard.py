import re

with open("dashboard.html", "r") as f:
    content = f.read()

# Extract CSS
style_match = re.search(r'<style>(.*?)</style>', content, re.DOTALL)
if style_match:
    with open("frontend/dashboard.css", "w") as f:
        f.write(style_match.group(1).strip())
    content = content.replace(style_match.group(0), '<link rel="stylesheet" href="/static/dashboard.css">')

# Extract JS
script_matches = list(re.finditer(r'<script>(.*?)</script>', content, re.DOTALL))
if script_matches:
    last_script = script_matches[-1]
    with open("frontend/dashboard.js", "w") as f:
        f.write(last_script.group(1).strip())
    content = content.replace(last_script.group(0), '<script src="/static/dashboard.js"></script>')

with open("frontend/dashboard.html", "w") as f:
    f.write(content)

print("Split successfully.")
