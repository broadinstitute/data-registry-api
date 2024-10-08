import csv
import random

def generate_data(num_rows, filename='random_data.csv'):
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        # Write header
        writer.writerow(['CHR', 'BP', 'OA', 'EA', 'EAF', 'BETA', 'SE', 'P', 'ODDS_RATIO','ODDS_RATIO_LB',
                         'ODDS_RATIO_UB','N'])

        # Define possible values for EA
        ea_values = ['A', 'C', 'T', 'G', 'D', 'I']

        # Generate random rows
        for _ in range(num_rows):
            chr_value = random.randint(1, 26)  # CHR (1-26)
            bp_value = random.randint(1, 1_000_000)  # BP (positive int, upper limit as example)
            oa_value = random.choice(ea_values)  # EA (must be one of 'A', 'C', 'T', 'G', 'D', 'I')
            ea_value = random.choice(ea_values)
            eaf = round(random.uniform(0, 1), 6)
            p_value = round(random.uniform(0, 1), 6)
            serr = round(random.uniform(0, 10), 6)
            beta_value = round(random.uniform(-5, 5), 6)
            odds_ratio = round(random.uniform(0, 10), 6)
            odds_ratio_lb = round(random.uniform(0, 10), 6)
            odds_ratio_ub = round(random.uniform(0, 10), 6)
            writer.writerow([chr_value, bp_value, oa_value, ea_value, eaf, beta_value, serr, p_value, odds_ratio,
                             odds_ratio_lb, odds_ratio_ub, random.randint(1, 10_000)])  # N (1-10,000)

# Example usage: generating 100,000 rows
generate_data(100000000)
