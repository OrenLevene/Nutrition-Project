from PIL import Image, ImageDraw

def process_logo():
    # Load the logo (assume it has a transparent background and white lines)
    img_path = r"c:\Users\Oren Arie Levene\Nutrition Project\src\web_app\static\images\logo.png"
    img = Image.open(img_path).convert("RGBA")
    
    # We will flood-fill the center pixel with green (#10b981)
    target_color = (16, 185, 129, 255)
    
    # The center pixel should be empty (transparent)
    w, h = img.size
    cx, cy = w // 2, h // 2
    
    # Check if the center pixel is mostly transparent
    center_pixel = img.getpixel((cx, cy))
    if center_pixel[3] < 50: # if alpha is low
        ImageDraw.floodfill(img, (cx, cy), target_color, thresh=50)
        img.save(img_path)
        print("Successfully bucket-filled the center of the logo!")
    else:
        print("Center pixel is not empty! Need a different coordinate.")
        print(f"Center pixel: {center_pixel}")

if __name__ == "__main__":
    process_logo()
