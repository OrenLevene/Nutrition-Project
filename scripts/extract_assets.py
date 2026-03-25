from PIL import Image
import os

# Paths
SOURCE_IMAGE = r"C:\Users\Oren Arie Levene\.gemini\antigravity\brain\f182eeb8-584d-46d1-ae18-7ea35cf667b1\hero_final_v2_1770494104582.png"
DEST_DIR = r"src\web_app\static\images"

def extract_assets():
    if not os.path.exists(SOURCE_IMAGE):
        print(f"Error: Source image not found at {SOURCE_IMAGE}")
        return

    try:
        img = Image.open(SOURCE_IMAGE)
        width, height = img.size
        print(f"Source Image Size: {width}x{height}")

        # 1. Extract Logo (Top Left "N NutriShop")
        # Approximate coordinates based on standard hero layout visual
        # Logo is usually top left, roughly 50x50 to 200x80 area?
        # Let's crop a generous safe area and then maybe trim?
        # Or just crop the "N" icon if that's what we want.
        # The user said "nice N logo like this".
        # Let's crop the N mark.
        # Assuming standard header height ~100px.
        # Let's take a guess at coordinates or just crop the icon.
        # 1024x1024 image usually? No, generated images are often landscape now.
        # Let's assume the logo is at roughly (50, 40) to (100, 90) or similar.
        # Better strategy: The user wants "make it look more like this".
        # I'll crop the Basket from the Right side.
        # And the "N" from the top left.
        
        # Basket: Right half, bottom 2/3?
        # Let's define crop box for basket. 
        # It's on the right.
        # Let's retry generating? No, user wants SAME image.
        # I'll try to find the bounding box of non-dark pixels?
        # The background is dark gradient.
        # It's risky to auto-crop.
        # I'll take the right 50% of the image for the basket?
        # The basket is distinct.
        
        # Let's just crop the right half for the basket image.
        # crop(left, top, right, bottom)
        basket_crop = img.crop((width // 2, 0, width, height))
        
        # Save exact basket
        basket_path = os.path.join(DEST_DIR, "hero_basket_exact.png")
        basket_crop.save(basket_path)
        print(f"Saved basket to {basket_path}")

        # Logo: Top left corner.
        # Let's crop the top left 300x150 region?
        logo_crop = img.crop((40, 40, 300, 120)) 
        # This is a guess. 
        # Ideally I'd use corner detection but that's overkill.
        # Let's try to grab the favicon N.
        # Actually I already have a favicon generated `nutrishop_icon_...`.
        # User said "good logo".
        # Maybe they want the exact one in the image.
        # It's safer to use the one I generated in Step 110?
        # The user approved `hero_final_v2` in Step 108.
        # The favicon in Step 110 was generated AFTER.
        # Maybe they prefer the one inside the hero image?
        # I will crop the one from the hero image.
        
        logo_path = os.path.join(DEST_DIR, "logo_exact.png")
        logo_crop.save(logo_path)
        print(f"Saved logo to {logo_path}")

    except Exception as e:
        print(f"Failed to process image: {e}")

if __name__ == "__main__":
    extract_assets()
