import re

with open('frontend/index.html', 'r', encoding='utf-8') as f:
    html = f.read()

# Add VT323 to index.html
if 'family=VT323' not in html:
    html = html.replace('<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&display=swap"\n        rel="stylesheet">',
                        '<link href="https://fonts.googleapis.com/css2?family=VT323&display=swap" rel="stylesheet">')
    html = html.replace("<link href=\"https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&display=swap\" rel=\"stylesheet\">",
                        '<link href="https://fonts.googleapis.com/css2?family=VT323&display=swap" rel="stylesheet">')

# Modify font families in index.html
html = re.sub(r"font-family:\s*'Inter',\s*-apple-system,\s*BlinkMacSystemFont,\s*sans-serif;", "font-family: 'VT323', monospace; text-transform: uppercase;", html)

# Border radii
html = re.sub(r'--radius-lg:\s*20px;', '--radius-lg: 0px;', html)
html = re.sub(r'--radius-xl:\s*28px;', '--radius-xl: 0px;', html)

# Hard borders
html = re.sub(r'border:\s*1px\s*solid\s*var\(--border-subtle\);', 'border: 2px solid var(--border-subtle);', html)
html = re.sub(r'border-color:\s*rgba\(139,\s*92,\s*246,\s*0\.25\);', 'border-color: var(--accent-purple);', html)
html = re.sub(r'border-color:\s*rgba\(139,\s*92,\s*246,\s*0\.3\);', 'border-color: var(--accent-purple);', html)
html = re.sub(r'border-color:\s*rgba\(245,\s*158,\s*11,\s*0\.3\);', 'border-color: var(--accent-orange);', html)

# Box shadows into hard shadows
html = re.sub(r'box-shadow:\s*0\s*20px\s*60px\s*rgba\(0,\s*0,\s*0,\s*0\.4\)[^;]*;', 'box-shadow: 6px 6px 0px rgba(0,0,0,0.8);', html)
html = re.sub(r'box-shadow:\s*0\s*4px\s*20px\s*rgba\(139,\s*92,\s*246,\s*0\.3\);', 'box-shadow: 4px 4px 0px rgba(0,0,0,0.8);', html)

# Backgrounds
html = re.sub(r'background:\s*var\(--bg-surface\);', 'background: var(--bg-base);', html)
html = re.sub(r'backdrop-filter:\s*blur\(20px\);', '/* backdrop-filter removed */', html)

# Font sizes up
html = re.sub(r'font-size:\s*11px;', 'font-size: 14px;', html)
html = re.sub(r'font-size:\s*14px;', 'font-size: 18px;', html)
html = re.sub(r'font-size:\s*17px;', 'font-size: 22px;', html)
html = re.sub(r'font-size:\s*18px;', 'font-size: 24px;', html)
html = re.sub(r'font-size:\s*20px;', 'font-size: 26px;', html)
html = re.sub(r'font-size:\s*22px;', 'font-size: 28px;', html)
html = re.sub(r'font-size:\s*26px;', 'font-size: 32px;', html)

with open('frontend/index.html', 'w', encoding='utf-8') as f:
    f.write(html)

print("index.html pixelated")

# Fix map in dashboard.css
with open('static/dashboard.css', 'r', encoding='utf-8') as f:
    css = f.read()

# Force map to be visible just in case the JS observer is failing
if '.leaflet-container' not in css:
    css += "\n\n/* Fix Leaflet Map */\n.map-section {\n    opacity: 1 !important;\n    transform: none !important;\n}\n"
    css += "#map {\n    display: block !important;\n    opacity: 1 !important;\n    height: 500px !important;\n    min-height: 500px !important;\n}\n"
    css += ".leaflet-container {\n    text-transform: none !important;\n    font-family: Arial, sans-serif !important;\n}\n"
    css += ".leaflet-tile { visibility: visible !important; opacity: 1 !important; display: block !important; }\n"

with open('static/dashboard.css', 'w', encoding='utf-8') as f:
    f.write(css)
    
print("dashboard.css map fixed")
