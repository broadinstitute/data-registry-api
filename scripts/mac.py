import random

def generate_mac():
    # Generate a random MAC address, ensuring the second hex digit is 2, 6, A, or E
    # This ensures the address is locally administered and unicast.
    mac = [0x02, 0x00, 0x00, 0x00, 0x00, 0x00]
    for i in range(1, 6):
        mac[i] = random.randint(0x00, 0xff)
    # Format the MAC address as colon-separated hex values
    return ':'.join(map(lambda x: "%02x" % x, mac))

print(generate_mac())
