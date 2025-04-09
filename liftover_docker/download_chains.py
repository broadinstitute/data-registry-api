from liftover import get_lifter

print("Downloading hg38 to hg19 chain file...")
lifter = get_lifter('hg38', 'hg19')
print(f"Chain file downloaded")
