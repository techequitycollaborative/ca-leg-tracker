"""
General text manipulation functions for database population
"""
import re


def transform_name(name):
    # Re-formats legislator names
    parts = re.split(r'\s+', name.replace(',', ''))
    no_title = len(name.split(",")) == 1
    if no_title:
        return f"{parts[-1]}, {' '.join(parts[0:-1])}"
    else:
        return f"{parts[-2]}, {' '.join(parts[0:-2])}, {parts[-1].replace(',', '')}"


def main():
    return


if __name__ == "__main__":
    main()
