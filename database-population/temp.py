"""
Transforms names to 'directory' format:
Jane Doe => Doe, Jane
Jane Angelina Doe => Doe, Jane Angelina
Jane A. Doe => Doe, Jane A.
Jane Doe, M.D. => Doe, Jane, M.D.
"""
import csv
import re
from collections import defaultdict


def detect_name_types(data):
    word_counts = defaultdict(list)
    for line in data:
        content = ",".join(line.strip().replace('"', '').split(",")[1:])
        word_counts[len(content.split(" "))].append(content)
    # print(f"name lengths range from {min(word_counts.keys())} to {max(word_counts.keys())}")
    # print(word_counts[3])
    for n in word_counts[3]:
        print(n, "=>", transform_name(n))
    return


def transform_name(name):
    parts = re.split(r'\s+', name.replace(',', ''))
    no_title = len(name.split(",")) == 1
    if no_title:
        return f"{parts[-1]}, {' '.join(parts[0:-1])}"
    else:
        return f"{parts[-2]}, {' '.join(parts[0:-2])}, {parts[-1].replace(',', '')}"


def main():
    input_file = 'exported_names.csv'
    output_file = 'transformed_names.csv'

    with open(input_file, mode='r', newline='', encoding='utf-8') as infile, \
        open(output_file, mode='w', newline='', encoding='utf-8') as outfile:
        
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for row in reader:
            row['name'] = transform_name(row['name'])
            writer.writerow(row)
    return


if __name__ == "__main__":
    main()
