#!/usr/bin/env python3
"""
Convert TSX file to gh-pages HTML for Fade Finder app.
Usage: python3 convert_tsx_to_html.py <tsx_file>
"""
import sys
import os

def convert(tsx_path, html_path):
    with open(tsx_path, 'r') as f:
        tsx_content = f.read()

    html_header = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Fade Finder - Capper Consensus Tracker</title>
  <script crossorigin src="https://unpkg.com/react@18/umd/react.production.min.js"></script>
  <script crossorigin src="https://unpkg.com/react-dom@18/umd/react-dom.production.min.js"></script>
  <script src="https://unpkg.com/@babel/standalone/babel.min.js"></script>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: 'Segoe UI', system-ui, sans-serif; }
  </style>
</head>
<body>
  <div id="root"></div>
  <script type="text/babel">
const { useState, useMemo, useEffect, useCallback, useRef, memo } = React;
"""

    html_footer = """

const root = ReactDOM.createRoot(document.getElementById('root'));
root.render(<CapperConsensusTracker />);
  </script>
</body>
</html>"""

    # Remove React import and export default from TSX
    lines = tsx_content.split('\n')

    # Skip the import line
    js_lines = []
    for line in lines:
        if line.startswith('import React'):
            continue
        js_lines.append(line)

    js_content = '\n'.join(js_lines)

    # Replace 'export default function CapperConsensusTracker' with 'function CapperConsensusTracker'
    js_content = js_content.replace('export default function CapperConsensusTracker', 'function CapperConsensusTracker')

    html_content = html_header + js_content + html_footer

    with open(html_path, 'w') as f:
        f.write(html_content)

    print(f"Written {len(html_content)} characters to {html_path}")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python3 convert_tsx_to_html.py <tsx_file>")
        print("Output will be written to gh-pages/index.html")
        sys.exit(1)
    
    tsx_path = sys.argv[1]
    script_dir = os.path.dirname(os.path.abspath(__file__))
    html_path = os.path.join(script_dir, 'gh-pages', 'index.html')
    
    convert(tsx_path, html_path)
