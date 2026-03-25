from PIL import Image

def remove_background(img_path):
    img = Image.open(img_path).convert("RGBA")
    data = img.getdata()
    
    # Get the background color from the top-left pixel
    bg_color = data[0]
    
    # We will compute a tolerance to remove pixels similar to the bg
    # Background in the image is likely a dark uniform color.
    tolerance = 15
    
    new_data = []
    for item in data:
        # Check if the pixel is close to bg_color
        if (abs(item[0] - bg_color[0]) <= tolerance and
            abs(item[1] - bg_color[1]) <= tolerance and
            abs(item[2] - bg_color[2]) <= tolerance):
            # Replace with transparent pixel
            new_data.append((255, 255, 255, 0))
        else:
            new_data.append(item)
            
    img.putdata(new_data)
    img.save(img_path)
    print(f"Successfully processed {img_path}")

if __name__ == "__main__":
    remove_background(r"c:\Users\Oren Arie Levene\Nutrition Project\src\web_app\static\images\hero_basket.png")
