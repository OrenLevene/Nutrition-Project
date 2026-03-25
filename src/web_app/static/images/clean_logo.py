from PIL import Image, ImageDraw
import os

def fix_logo():
    base_dir = r'c:\Users\Oren Arie Levene\Nutrition Project\src\web_app\static\images'
    src = os.path.join(base_dir, 'logo_exact.png')
    dst = os.path.join(base_dir, 'logo.png')
    
    # Load original unadulterated logo
    img = Image.open(src).convert("RGBA")
    w, h = img.size
    
    # Fill the center
    # The center is around (w//2, h//2)
    # The previous script used seed (159, 168) which worked for filling but leaked
    GREEN = (16, 185, 129, 255)
    
    # Use flood fill with strict threshold
    ImageDraw.floodfill(img, (159, 168), GREEN, thresh=30)
    
    # Now explicitly erase the "green triangle leak" which is usually outside the hexagon
    # We can just clear any green pixels that are too far from the center or in the corners
    pixels = img.load()
    for y in range(h):
        for x in range(w):
            r, g, b, a = pixels[x, y]
            if g > 150 and r < 80 and b < 150 and a > 100:
                # Top right corner leak check
                if x > w * 0.7 or y < h * 0.2:
                    pixels[x, y] = (0, 0, 0, 0)
                # Let's cleanly clear ALL corners just in case
                # The hexagon is in the center. If it's too far to the edges, it's a leak
                # Top left
                if x < w * 0.3 and y < h * 0.2:
                    pixels[x, y] = (0, 0, 0, 0)
                # Bottom left
                if x < w * 0.3 and y > h * 0.8:
                    pixels[x, y] = (0, 0, 0, 0)
                # Bottom right
                if x > w * 0.7 and y > h * 0.8:
                    pixels[x, y] = (0, 0, 0, 0)

    img.save(dst)
    print("Fixed fill applied to logo, corners cleaned.")
    
    # Update favicon
    fav_dst = os.path.join(base_dir, 'favicon.png')
    favicon = img.resize((32, 32), Image.LANCZOS)
    favicon.save(fav_dst)
    print("Favicon updated.")

if __name__ == "__main__":
    fix_logo()
