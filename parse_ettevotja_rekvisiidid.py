import ijson
import hashlib
import csv

# File name where ettevõtja rekvisiidid are
file_path = "ettevotja_rekvisiidid__yldandmed.json"

count = 0

# create raw_companies.csv file
with open("raw_companies.csv", "w") as f:
    f.write("company_id,company_name,registration_code,registration_date,"
            "sector_code,valid_from,valid_to,is_current\n")

# create raw_companies.csv file
with open("raw_sectors.csv", "w") as f:
    f.write("sector_code,sector_name\n")

# Store here the sectors to write to raw_sectors.csv file
sectors_dict = {}

# function for generating company key
def generate_surrogate_key(row):
    # Combine natural keys into a single string
    raw_string = f"{row['company_name']}_{row['sector_valid_from']}"
    # Generate deterministic 16-character hex hash
    return hashlib.md5(raw_string.encode("utf-8")).hexdigest()[:16]

with open(file_path, "rb") as f:
    # 'item' tells ijson to parse individual objects inside the root array
    records = ijson.items(f, "item")
    
    for record in records:
        company_name = record.get("nimi")
        reg_code = record.get("ariregistri_kood")
        reg_date = record.get("yldandmed").get("esmaregistreerimise_kpv")
        sectors_raw = record.get("yldandmed").get("teatatud_tegevusalad")

        main_sectors = {}

        # if no sector defined then skip, we don't have any use of such company for our project
        if not sectors_raw:
            continue

        # iterate over sectors
        for sector in sectors_raw:

            # Check if sector is main sector and only then process
            if sector["on_pohitegevusala"]:
                emtak_code = sector["emtak_kood"]
                emtak_name = sector["emtak_tekstina"]
                sector_start_date = sector["algus_kpv"]
                sector_end_date = sector["lopp_kpv"]

                # Add sector to sectors_dict, to be inserted into raw_sectors.csv file later
                sectors_dict[emtak_code] = [emtak_code, emtak_name]

                # prepare company item for later processing, will add to main_sectors dictionary
                item = {
                    "company_name": company_name,
                    "registration_code": reg_code,
                    "registration_date": reg_date,
                    "sector_code": emtak_code,
                    "sector_name": emtak_name,
                    "sector_valid_from": sector_start_date,
                    "sector_valid_to": sector_end_date,
                    "is_current": 0,
                }

                # Set value for is_current, if sector is currently valid
                if item["sector_valid_to"] is None:
                    item["is_current"] = 1

                # generate ID from a combination of values
                unique_id = generate_surrogate_key(item)
                item["CompanyID"] = unique_id

                main_sectors[emtak_code] = item
        
        count_pt = 0

        rows_to_write = []

        # prep cvs rows to be written into raw_companies.csv file
        for key, pt in main_sectors.items():

            company_row = [
                pt["CompanyID"],
                pt["company_name"],
                pt["registration_code"],
                pt["registration_date"],
                pt["sector_code"],
                pt["sector_valid_from"],
                pt["sector_valid_to"],
                pt["is_current"],
            ]
            rows_to_write.append(company_row)

            row_to_write = (f'{pt["CompanyID"]},{pt["company_name"]},{pt["registration_code"]},{pt["registration_date"]},'
                            f'{pt["sector_code"]},"{pt["sector_name"]}",{pt["sector_valid_from"]},{pt["sector_valid_to"]},'
                            f'{pt["is_current"]}')
            print(row_to_write)

            count_pt += 1

        # append row to raw_companies.csv file, use pythons csv.write
        with open("raw_companies.csv", "a", newline="", encoding="utf-8") as f_companies:
            writer = csv.writer(f_companies)
            writer.writerows(rows_to_write)
        
        count += 1

print(f'sectors count={len(sectors_dict.keys())}')

# Create a list of lists (sector id + name as items) to write into csv file
sectors_to_write = [value for key, value in sectors_dict.items()]

# append row to raw_sectors.csv file
with open("raw_sectors.csv", "a", newline="", encoding="utf-8") as f_sectors:
    writer = csv.writer(f_sectors)
    writer.writerows(sectors_to_write)

print(f"END COUNT={count}")
