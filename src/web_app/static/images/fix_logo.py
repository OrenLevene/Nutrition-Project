"""
Fix the logo - v3: Use aggressive barrier and bounding box constraint.
"""
from PIL import Image
import os

LOGO_PATH = os.path.join(os.path.dirname(__file__), "logo.png")
FAVICON_PATH = os.path.join(os.path.dirname(__file__), "favicon.png")

GREEN = (16, 185, 129, 255)

def main():
    img = Image.open(LOGO_PATH).convert("RGBA")
    w, h = img.size
    print(f"Image size: {w}x{h}")
    
    # Step 1: Revert ALL green pixels to transparent
    pixels = img.load()
    reverted = 0
    for y in range(h):
        for x in range(w):
            r, g, b, a = pixels[x, y]
            if g > 100 and r < 80 and b < 180 and a > 100:
                pixels[x, y] = (0, 0, 0, 0)
                reverted += 1
    print(f"Reverted {reverted} green pixels.")
    
    # Step 2: Build barrier from white/opaque pixels with 3px expansion
    opaque = set()
    for y in range(h):
        for x in range(w):
            r, g, b, a = pixels[x, y]
            if a > 80:  # any non-transparent pixel is part of the design
                opaque.add((x, y))
    
    barrier = set()
    for (bx, by) in opaque:
        for dx in range(-3, 4):  # 3px radius
            for dy in range(-3, 4):
                nx, ny = bx + dx, by + dy
                if 0 <= nx < w and 0 <= ny < h:
                    barrier.add((nx, ny))
    
    print(f"Barrier: {len(barrier)} pixels ({len(opaque)} opaque source)")
    
    # Step 3: BFS flood fill from center, constrained to avoid edges
    seed = (w // 2, int(h * 0.45))
    # Find nearest transparent non-barrier pixel
    if pixels[seed[0], seed[1]][3] > 50 or seed in barrier:
        found = False
        for r in range(1, 50):
            for dy in range(-r, r+1):
                for dx in range(-r, r+1):
                    tx, ty = seed[0]+dx, seed[1]+dy
                    if 0 <= tx < w and 0 <= ty < h:
                        if pixels[tx, ty][3] < 50 and (tx, ty) not in barrier:
                            seed = (tx, ty)
                            found = True
                            break
                if found: break
            if found: break
    
    print(f"Seed: {seed}")
    
    visited = set()
    queue = [seed]
    fill_pixels = []
    
    while queue:
        x, y = queue.pop(0)
        if (x, y) in visited:
            continue
        if x < 0 or x >= w or y < 0 or y >= h:
            continue
        if (x, y) in barrier:
            continue
        if pixels[x, y][3] > 50:  # not transparent
            continue
        
        visited.add((x, y))
        fill_pixels.append((x, y))
        
        queue.append((x+1, y))
        queue.append((x-1, y))
        queue.append((x, y+1))
        queue.append((x, y-1))
    
    print(f"Fill: {len(fill_pixels)} pixels")
    
    # Apply fill
    for x, y in fill_pixels:
        pixels[x, y] = GREEN
    
    img.save(LOGO_PATH)
    print(f"Saved logo")
    
    # Favicon
    favicon = img.resize((32, 32), Image.LANCZOS)
    favicon.save(FAVICON_PATH)
    print(f"Saved favicon")
    print("Done!")

if __name__ == "__main__":
    main()
