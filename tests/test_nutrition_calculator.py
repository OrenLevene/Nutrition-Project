
import subprocess
import sys

def run_test(age, gender, weight, height, activity, expected_tdee_min, expected_tdee_max):
    print(f"Testing: {age}y {gender}, {weight}kg, {height}cm, {activity}")
    cmd = [
        sys.executable, "-m", "src.calculator.engine",
        "--age", str(age),
        "--gender", gender,
        "--weight", str(weight),
        "--height", str(height),
        "--activity", activity
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print("FAIL: Process crashed")
        print(result.stderr)
        return False
        
    output = result.stdout
    # Very basic parsing to find TDEE
    tdee_line = [line for line in output.split('\n') if "TDEE" in line]
    if not tdee_line:
        print("FAIL: TDEE not found in output")
        return False
        
    tdee_val = float(tdee_line[0].split(':')[1].replace('kcal/day','').strip())
    
    if expected_tdee_min <= tdee_val <= expected_tdee_max:
        print(f"PASS: TDEE {tdee_val} is within {expected_tdee_min}-{expected_tdee_max}")
        return True
    else:
        print(f"FAIL: TDEE {tdee_val} not in range {expected_tdee_min}-{expected_tdee_max}")
        return False

# Test Case 1: 25M, 70kg, 175cm, Moderately Active
# BMR (Mifflin) = 10*70 + 6.25*175 - 5*25 + 5 = 700 + 1093.75 - 125 + 5 = 1673.75
# TDEE = 1673.75 * 1.55 = 2594.3
run_test(25, "male", 70, 175, "moderately_active", 2590, 2600)

# Test Case 2: 30F, 60kg, 165cm, Sedentary
# BMR = 10*60 + 6.25*165 - 5*30 - 161 = 600 + 1031.25 - 150 - 161 = 1320.25
# TDEE = 1320.25 * 1.2 = 1584.3
run_test(30, "female", 60, 165, "sedentary", 1580, 1590)

# Test Case 3: 5y Male (Child), 20kg, 110cm, Very Active
# BMR = 10*20 + 6.25*110 - 5*5 + 5 = 200 + 687.5 - 25 + 5 = 867.5
# TDEE = 867.5 * 1.725 = 1496.4
run_test(5, "male", 20, 110, "very_active", 1490, 1500)
