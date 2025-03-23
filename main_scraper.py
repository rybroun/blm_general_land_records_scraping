import requests
import json
from bs4 import BeautifulSoup
import time
import os
import re
import shutil
from urllib.parse import urlparse
from datetime import datetime
from parse_utilities import parse_patentees

def extract_accession_info(link_url):
    """Extract accession number and doc class from a patent detail URL"""
    match = re.search(r'accession=([^&]+)&docClass=([^&]+)', link_url)
    if match:
        return match.group(1), match.group(2)
    return None, None

def get_image_link(accession, doc_class, max_retries=10, retry_delay=3):
    """
    Get the actual image link for a patent by first visiting the details page.
    Will poll until the image is ready or max_retries is reached.
    
    Args:
        accession: The accession number
        doc_class: The document class
        max_retries: Maximum number of times to check if image is ready
        retry_delay: Seconds to wait between retries
    """
    # Print statement to track which accession we're processing
    print(f"Getting image link for accession {accession}, document class {doc_class}...")
    
    # First, visit the patent details page with the image tab
    details_url = f"https://glorecords.blm.gov/details/patent/default.aspx?accession={accession}&docClass={doc_class}#patentDetailsTabIndex=1"
    
    try:
        # Make the request with browser-like headers
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        }
        
        response = requests.get(details_url, headers=headers)
        response.raise_for_status()
        
        # Parse the HTML to find the getImage.ashx URL with the key
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Look for scripts that might contain the getImage.ashx URL
        scripts = soup.find_all('script')
        image_url = None
        
        for script in scripts:
            if script.string and 'getImage.ashx' in script.string:
                # Extract the URL using regex
                matches = re.findall(r'(https://glorecords\.blm\.gov/WebServices/getImage\.ashx\?[^"\']+)', script.string)
                if matches:
                    image_url = matches[0]
                    break
        
        if not image_url:
            print("Could not find getImage.ashx URL in the page")
            return None
        
        
        # Now poll the getImage.ashx endpoint until the image is ready or max_retries is reached
        for attempt in range(1, max_retries + 1):            
            # Make the request to the getImage.ashx endpoint
            image_response = requests.get(image_url, headers=headers)
            image_response.raise_for_status()
            
            # Parse the JSON response
            image_data = image_response.json()
            status = image_data.get("conversionStatus")
                        
            if status == "READY":
                image_link = image_data.get("imageFileLink")
                print(f"Successfully retrieved image link: {image_link}")
                return image_link
            elif status == "WORKING":
                if attempt < max_retries:
                    print(f"Image is still being processed. Waiting {retry_delay} seconds before checking again...")
                    time.sleep(retry_delay)
                else:
                    print("Maximum retries reached. Image is still being processed.")
                    return None
            else:
                print(f"Unexpected status: {status}")
                print(f"Error message: {image_data.get('errorMessage', 'No error message')}")
                return None
                
        return None  # This will only be reached if all retries fail
    except Exception as e:
        print(f"Error getting image link for {accession}: {e}")
        return None

def download_image(image_url, save_path):
    """Download an image file to the specified path"""
    try:
        response = requests.get(image_url, stream=True)
        response.raise_for_status()
        
        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        return True
    except Exception as e:
        print(f"Error downloading image {image_url}: {e}")
        return False

def get_filename_from_url(url):
    """Extract filename from URL"""
    parsed_url = urlparse(url)
    path = parsed_url.path
    return os.path.basename(path)

def convert_to_printer_friendly(url):
    """Convert a detail page URL to printer-friendly version"""
    if not url:
        return url
    
    # Replace 'default.aspx' with 'default_pf.aspx'
    return url.replace('/default.aspx', '/default_pf.aspx')

def get_search_results_table(state_abbr, county_id, county_name):
    """Fetch the search results table once and return it for processing"""
    
    url = f"https://glorecords.blm.gov/results/default_pf.aspx?searchCriteria=type=patent%7Cst={state_abbr}%7Ccty={county_id}%7Csp=true%7Csw=true%7Csadv=false&resultsTabIndex=0"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    try:
        print(f"Fetching search results table for {state_abbr}, county {county_name} ({county_id})...")
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the records table - adjust selector as needed
        table = soup.find('table', {'class': 'resultsPF'})
        
        if not table:
            print(f"No table found for {state_abbr}, county {county_name} from {url}")
            return None
        
        rows = table.find_all('tr')
        
        # Return all rows except the header
        return rows[1:] if len(rows) > 1 else []
        
    except Exception as e:
        print(f"Error fetching search results table for {state_abbr}, county {county_name}: {e}")
        return None

def process_record_from_rows(rows, index, state_abbr, county_id):
    """Process a single record from the table rows at the given index"""
    
    if index >= len(rows):
        print(f"Index {index} is out of range. Table has {len(rows)} records.")
        return None, 0
    
    try:
        # Get the row at the specified index
        row = rows[index]
        cols = row.find_all('td')
        
        # Extract data from the row
        accession = cols[0].text.strip()
        
        # Extract link from first column
        link_element = cols[0].find('a')
        link_url = link_element['href'] if link_element else None
        
        if link_url and not link_url.startswith('http'):
            # Fix the relative URL properly by handling the "../" pattern
            if link_url.startswith('../'):
                link_url = link_url[3:]  # Remove the '../' prefix
            
            link_url = f"https://glorecords.blm.gov/{link_url}"
        
        # Convert to printer-friendly version
        link_url = convert_to_printer_friendly(link_url)
            
        # Extract accession and doc class from link
        doc_accession, doc_class = extract_accession_info(link_url) if link_url else (None, None)
        
        # Get image link
        image_link = get_image_link(doc_accession, doc_class) if doc_accession and doc_class else None
        
        # Create record with initial parcel
        record = {
            "basic_info": {
                "accession": accession,
                "doc_class": doc_class,
                "type": "Land Patent",
                "cancelled": None
            },
            "misc_info":{
                "land_office": None,
                "us_reservations": None,
                "mineral_reservations": None,
                "tribe": None,
                "militia": None,
                "state_in_favor_of": None,
                "authority": None,
            },
            "survey_info":{
                "total_acres": None,
                "survey_date": None,
                "geographic_name": None,
                "metes_and_bounds": None,
            },
            "document_numbers":{
                "doc_number": cols[3].text.strip(),
                "misc_doc_number": None,
                "blm_serial_number": None,
                "indian_allotment_number": None,
                "coal_entry_number": None
            },
            "dates": {
                "issue_date": cols[2].text.strip()
            },
            "parcels": [
                {
                    "meridian": cols[5].text.strip(),
                    "township_range": cols[6].text.strip(),
                    "aliquots": cols[7].text.strip(),
                    "section_number": cols[8].text.strip(),
                    "county": cols[9].text.strip(),
                }
            ],
            "location": {
                "state": cols[4].text.strip(),
                "state_name": None,
                "county_id": county_id,
                "legal_description": None,
            },
            "people": {
                "patentees": parse_patentees(cols[1]),
                "signatories": [],
                "military_rank": None,
            },
            "document_access": {
                "detail_link": link_url,
                "image_link": image_link,
                "local_image_path": None
            },
            "metadata": {
                "scraped_date": datetime.now().strftime("%Y-%m-%d"),
                "last_updated": datetime.now().strftime("%Y-%m-%d")
            }
        }
        
        # Check if there are additional parcels for this record
        # We need to look at subsequent rows with the same accession
        rows_consumed = 1
        current_index = index + 1  # Start from the next row
        
        while current_index < len(rows):
            next_row = rows[current_index]
            next_cols = next_row.find_all('td')
            next_accession = next_cols[0].text.strip()
            
            # If accession matches, it's an additional parcel for the same record
            if next_accession == accession:
                additional_parcel = {
                    "meridian": next_cols[5].text.strip(),
                    "township_range": next_cols[6].text.strip(),
                    "aliquots": next_cols[7].text.strip(),
                    "section_number": next_cols[8].text.strip(),
                    "county": next_cols[9].text.strip(),
                }
                record["parcels"].append(additional_parcel)
                current_index += 1
                rows_consumed += 1
            else:
                # Different accession, so stop looking for additional parcels
                break
        
        return record, rows_consumed
        
    except Exception as e:
        print(f"Error processing record at index {index}: {e}")
        return None, 0

def enhance_record_with_details(record, state_name):
    """Enhance record with additional information from the detail page"""
    detail_url = record["document_access"]["detail_link"]
    print(f"Enhancing record with details for {detail_url}")
    if not detail_url:
        print("No detail URL available for this record")
        return record
    
    try:
        print(f"Fetching details for accession {record['basic_info']['accession']}...")
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        
        response = requests.get(detail_url, headers=headers)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Update state name
        record["location"]["state_name"] = state_name
        
        # Create a consolidated legal description from all parcels
        legal_descriptions = []
        for parcel in record["parcels"]:
            parcel_desc = f"{parcel['aliquots']} of Section {parcel['section_number']}, {parcel['township_range']}, {parcel['meridian']} Meridian, {parcel['county']} County"
            legal_descriptions.append(parcel_desc)
        
        record["location"]["legal_description"] = "; ".join(legal_descriptions)
        
        # Extract data from detail page
        try:
            
            
            ### Parse Misc Info Table

            # cancelled - find by ID and get its text directly
            cancelled_element = soup.find(id="cancelled")
            if cancelled_element:
                record["basic_info"]["cancelled"] = cancelled_element.text.strip()
            # Fallback to the original method if ID-based search fails
            else:
                cancelled_label = soup.find(string=re.compile("Cancelled", re.IGNORECASE))
                if cancelled_label and cancelled_label.parent and cancelled_label.parent.find_next_sibling():
                    record["basic_info"]["land_office"] = cancelled_label.parent.find_next_sibling().text.strip()
            
            # Land Office - find by ID and get its text directly
            land_office_element = soup.find(id="landOffice")
            if land_office_element:
                record["misc_info"]["land_office"] = land_office_element.text.strip()
            # Fallback to the original method if ID-based search fails
            else:
                land_office_label = soup.find(string=re.compile("Land Office", re.IGNORECASE))
                if land_office_label and land_office_label.parent and land_office_label.parent.find_next_sibling():
                    record["misc_info"]["land_office"] = land_office_label.parent.find_next_sibling().text.strip()
            
            # Authority - find by ID and get its text directly
            authority_element = soup.find(id="authority")
            if authority_element:
                record["misc_info"]["authority"] = authority_element.text.strip()
            # Fallback to the original method if ID-based search fails
            else:
                authority_label = soup.find(string=re.compile("Authority", re.IGNORECASE))
                if authority_label and authority_label.parent and authority_label.parent.find_next_sibling():
                    record["misc_info"]["authority"] = authority_label.parent.find_next_sibling().text.strip()
            
            # Document Class - find by ID and get its text directly
            doc_class_element = soup.find(id="documentType")
            if doc_class_element:
                record["basic_info"]["doc_class"] = doc_class_element.text.strip()
            # Fallback to the original method if ID-based search fails
            else:
                doc_class_label = soup.find(string=re.compile("Document Type", re.IGNORECASE))
                if doc_class_label and doc_class_label.parent and doc_class_label.parent.find_next_sibling():
                    record["basic_info"]["doc_class"] = doc_class_label.parent.find_next_sibling().text.strip()
            
            # State - find by ID and get its text directly
            state_element = soup.find(id="stateName")
            if state_element:
                record["location"]["state"] = state_element.text.strip()
            # Fallback to the original method if ID-based search fails
            else:
                state_label = soup.find(string=re.compile("State", re.IGNORECASE))
                if state_label and state_label.parent and state_label.parent.find_next_sibling():
                    record["location"]["state"] = state_label.parent.find_next_sibling().text.strip()
            
            # Accession - find by ID and get its text directly
            accession_element = soup.find(id="accessionNr")
            if accession_element:
                record["basic_info"]["accession"] = accession_element.text.strip()
            # Fallback to the original method if ID-based search fails
            else:
                accession_label = soup.find(string=re.compile("Accession", re.IGNORECASE))
                if accession_label and accession_label.parent and accession_label.parent.find_next_sibling():
                    record["basic_info"]["accession"] = accession_label.parent.find_next_sibling().text.strip()
            
            # Issue Date - find by ID and get its text directly
            issue_date_element = soup.find(id="issueDate")
            if issue_date_element:
                record["dates"]["issue_date"] = issue_date_element.text.strip()
            # Fallback to the original method if ID-based search fails
            else:
                issue_date_label = soup.find(string=re.compile("Issue Date", re.IGNORECASE))
                if issue_date_label and issue_date_label.parent and issue_date_label.parent.find_next_sibling():
                    record["dates"]["issue_date"] = issue_date_label.parent.find_next_sibling().text.strip()

            # US Reservations - find by ID and get its text directly
            us_reservations_element = soup.find(id="usReservations")
            if us_reservations_element:
                record["misc_info"]["us_reservations"] = us_reservations_element.text.strip()
            # Fallback to the original method if ID-based search fails
            else:
                us_reservations_label = soup.find(string=re.compile("US Reservations", re.IGNORECASE))
                if us_reservations_label and us_reservations_label.parent and us_reservations_label.parent.find_next_sibling():
                    record["misc_info"]["us_reservations"] = us_reservations_label.parent.find_next_sibling().text.strip()

            # Mineral Reservations - find by ID and get its text directly
            mineral_reservations_element = soup.find(id="mineralReservations")
            if mineral_reservations_element:
                record["misc_info"]["mineral_reservations"] = mineral_reservations_element.text.strip()
            # Fallback to the original method if ID-based search fails
            else:
                mineral_reservations_label = soup.find(string=re.compile("Mineral Reservations", re.IGNORECASE))
                if mineral_reservations_label and mineral_reservations_label.parent and mineral_reservations_label.parent.find_next_sibling():
                    record["misc_info"]["mineral_reservations"] = mineral_reservations_label.parent.find_next_sibling().text.strip()

            # Tribe - find by ID and get its text directly
            tribe_element = soup.find(id="tribe")
            if tribe_element:
                record["misc_info"]["tribe"] = tribe_element.text.strip()
            # Fallback to the original method if ID-based search fails
            else:
                tribe_label = soup.find(string=re.compile("Tribe", re.IGNORECASE))
                if tribe_label and tribe_label.parent and tribe_label.parent.find_next_sibling():
                    record["misc_info"]["tribe"] = tribe_label.parent.find_next_sibling().text.strip()
                    
            # Militia - find by ID and get its text directly
            militia_element = soup.find(id="militia")
            if militia_element:
                record["misc_info"]["militia"] = militia_element.text.strip()
            # Fallback to the original method if ID-based search fails
            else:
                militia_label = soup.find(string=re.compile("Militia", re.IGNORECASE))
                if militia_label and militia_label.parent and militia_label.parent.find_next_sibling():
                    record["misc_info"]["militia"] = militia_label.parent.find_next_sibling().text.strip()

            # State in favor of - find by ID and get its text directly
            state_in_favor_of_element = soup.find(id="stateInFavorOf")
            if state_in_favor_of_element:
                record["misc_info"]["state_in_favor_of"] = state_in_favor_of_element.text.strip()
            # Fallback to the original method if ID-based search fails
            else:
                state_in_favor_of_label = soup.find(string=re.compile("State In Favor Of:", re.IGNORECASE))
                if state_in_favor_of_label and state_in_favor_of_label.parent and state_in_favor_of_label.parent.find_next_sibling():
                    record["misc_info"]["state_in_favor_of"] = state_in_favor_of_label.parent.find_next_sibling().text.strip()

            # Document Nr - find by ID and get its text directly
            document_nr_element = soup.find(id="documentNr")
            if document_nr_element:
                record["document_numbers"]["doc_number"] = document_nr_element.text.strip()
            # Fallback to the original method if ID-based search fails
            else:
                document_nr_label = soup.find(string=re.compile("Document Nr", re.IGNORECASE))
                if document_nr_label and document_nr_label.parent and document_nr_label.parent.find_next_sibling():
                    record["document_numbers"]["doc_number"] = document_nr_label.parent.find_next_sibling().text.strip()
            
            # Total Acres - find by ID and get its text directly
            total_acres_element = soup.find(id="totalAcres")
            if total_acres_element:
                record["survey_info"]["total_acres"] = total_acres_element.text.strip()
            # Fallback to the original method if ID-based search fails
            else:
                total_acres_label = soup.find(string=re.compile("Total Acres", re.IGNORECASE))
                if total_acres_label and total_acres_label.parent and total_acres_label.parent.find_next_sibling():
                    record["survey_info"]["total_acres"] = total_acres_label.parent.find_next_sibling().text.strip()
            
            # Misc. Doc. Nr - find by ID and get its text directly
            misc_doc_nr_element = soup.find(id="miscDocumentNr")
            if misc_doc_nr_element:
                record["document_numbers"]["misc_doc_number"] = misc_doc_nr_element.text.strip()
            # Fallback to the original method if ID-based search fails
            else:
                misc_doc_nr_label = soup.find(string=re.compile("Misc. Doc. Nr", re.IGNORECASE))
                if misc_doc_nr_label and misc_doc_nr_label.parent and misc_doc_nr_label.parent.find_next_sibling():
                    record["document_numbers"]["misc_doc_number"] = misc_doc_nr_label.parent.find_next_sibling().text.strip()
            
            # Survey Date - find by ID and get its text directly
            survey_date_element = soup.find(id="surveyDate")
            if survey_date_element:
                record["survey_info"]["survey_date"] = survey_date_element.text.strip()
            # Fallback to the original method if ID-based search fails
            else:
                survey_date_label = soup.find(string=re.compile("Survey Date", re.IGNORECASE))
                if survey_date_label and survey_date_label.parent and survey_date_label.parent.find_next_sibling():
                    record["survey_info"]["survey_date"] = survey_date_label.parent.find_next_sibling().text.strip()
            
            # BLM Serial Nr - find by ID and get its text directly
            blm_serial_nr_element = soup.find(id="blmSerialNr")
            if blm_serial_nr_element:
                record["document_numbers"]["blm_serial_number"] = blm_serial_nr_element.text.strip()
            # Fallback to the original method if ID-based search fails
            else:
                blm_serial_nr_label = soup.find(string=re.compile("BLM Serial Nr", re.IGNORECASE))
                if blm_serial_nr_label and blm_serial_nr_label.parent and blm_serial_nr_label.parent.find_next_sibling():
                    record["document_numbers"]["blm_serial_number"] = blm_serial_nr_label.parent.find_next_sibling().text.strip()
            
            # Geographic Name - find by ID and get its text directly
            geographic_name_element = soup.find(id="geographicName")
            if geographic_name_element:
                record["survey_info"]["geographic_name"] = geographic_name_element.text.strip()
            # Fallback to the original method if ID-based search fails
            else:
                geographic_name_label = soup.find(string=re.compile("Geographic Name", re.IGNORECASE))
                if geographic_name_label and geographic_name_label.parent and geographic_name_label.parent.find_next_sibling():
                    record["survey_info"]["geographic_name"] = geographic_name_label.parent.find_next_sibling().text.strip()
            
            # Indian Allot. Nr - find by ID and get its text directly
            indian_allotment_nr_element = soup.find(id="indianAllotmentNr")
            if indian_allotment_nr_element:
                record["document_numbers"]["indian_allotment_number"] = indian_allotment_nr_element.text.strip()
            # Fallback to the original method if ID-based search fails
            else:
                indian_allotment_nr_label = soup.find(string=re.compile("Indian Allot. Nr", re.IGNORECASE))
                if indian_allotment_nr_label and indian_allotment_nr_label.parent and indian_allotment_nr_label.parent.find_next_sibling():
                    record["document_numbers"]["indian_allotment_number"] = indian_allotment_nr_label.parent.find_next_sibling().text.strip()
            
            # Metes/Bounds - find by ID and get its text directly
            metes_bounds_element = soup.find(id="metesBounds")
            if metes_bounds_element:
                record["survey_info"]["metes_and_bounds"] = metes_bounds_element.text.strip()
            # Fallback to the original method if ID-based search fails
            else:
                metes_bounds_label = soup.find(string=re.compile("Metes/Bounds", re.IGNORECASE))
                if metes_bounds_label and metes_bounds_label.parent and metes_bounds_label.parent.find_next_sibling():
                    record["survey_info"]["metes_and_bounds"] = metes_bounds_label.parent.find_next_sibling().text.strip()
            
            # Military Rank - find by text label since there's no ID
            military_rank_label = soup.find(string=re.compile("Military Rank", re.IGNORECASE))
            if military_rank_label and military_rank_label.parent and military_rank_label.parent.find_next_sibling():
                record["people"]["military_rank"] = military_rank_label.parent.find_next_sibling().text.strip()
            
            # Names section - contains both patentee and warrantee information
            names_element = soup.find(id="names")
            if names_element:
                # Use the updated parse_patentees function from parse_utilities
                people = parse_patentees(names_element)
                
                # Filter out patentees and store them in the people.patentees array
                patentees = [person for person in people if person["type"] == "patentee"]
                if patentees:
                    record["people"]["patentees"] = patentees
                
                # Optionally, you can also store other types of people in separate arrays
                warrantees = [person for person in people if person["type"] == "warrantee"]
                if warrantees:
                    record["people"]["warrantees"] = warrantees
                
                assignees = [person for person in people if person["type"] == "assignee"]
                if assignees:
                    record["people"]["assignees"] = assignees
            
        except Exception as detail_e:
            print(f"Error extracting specific detail: {detail_e}")
        
        return record
        
    except Exception as e:
        print(f"Error enhancing record with details: {e}")
        return record

def download_patent_image(record, images_dir):
    """Download the patent image and update the record with local path"""
    image_link = record["document_access"]["image_link"]
    print(image_link)
    
    if not image_link:
        print(f"No image link available for accession {record['basic_info']['accession']}")
        return
    
    try:
        # Create filename from the URL or accession
        filename = get_filename_from_url(image_link)
        if not filename:
            filename = f"{record['basic_info']['doc_class']}-{record['basic_info']['accession']}.pdf"
        
        # Full path to save the image
        image_save_path = os.path.join(images_dir, filename)
        
        # Download the image
        if download_image(image_link, image_save_path):
            print(f"Downloaded image to {image_save_path}")
            
            # Extract state and county for relative path
            state_abbr = record["location"]["state"]
            county_id = record["location"]["county_id"]
            
            # Update record with local path
            record["document_access"]["local_image_path"] = os.path.join(
                "images", state_abbr, county_id, filename
            )
    except Exception as e:
        print(f"Error downloading patent image: {e}")

def create_zip(directory, output_filename):
    """Create a zip file of a directory"""
    try:
        shutil.make_archive(output_filename, 'zip', directory)
        print(f"Created zip file: {output_filename}.zip")
        return True
    except Exception as e:
        print(f"Error creating zip file: {e}")
        return False

def count_unique_patents(table_rows):
    """Count the number of unique patents in the table rows"""
    if not table_rows:
        return 0
        
    unique_accessions = set()
    
    for row in table_rows:
        cols = row.find_all('td')
        if cols and len(cols) > 0:
            accession = cols[0].text.strip()
            unique_accessions.add(accession)
    
    return len(unique_accessions)

def main():
    # Load state and county data
    try:
        with open("state_counties.json", "r") as f:
            states_data = json.load(f)
    except Exception as e:
        print(f"Error loading state_counties.json: {e}")
        return
    
    # Create output directories
    base_dir = "land_records"
    json_dir = os.path.join(base_dir, "json")
    
    os.makedirs(base_dir, exist_ok=True)
    os.makedirs(json_dir, exist_ok=True)
    
    # Initialize county mapping data
    county_mapping = []
    
    # Process only one county from one state for testing
    for state in states_data:
        state_abbr = state["abbreviation"]
        state_name = state["state"]
        
        # Skip if no counties
        if not state["counties"]:
            continue
        
        # Create JSON directory for this state
        state_json_dir = os.path.join(json_dir, state_abbr)
        os.makedirs(state_json_dir, exist_ok=True)
            
        # Just process the first county
        county = state["counties"][0]
        county_id = county["id"]
        county_name = county["name"]
        
        # Create image directory for this county
        county_images_dir = os.path.join(base_dir, "images", state_abbr, county_id)
        os.makedirs(county_images_dir, exist_ok=True)
        
        # Define the output file path
        file_name = f"{state_abbr}_{county_id}_records.json"
        file_path = os.path.join(state_json_dir, file_name)
        
        # Fetch the search results table once
        table_rows = get_search_results_table(state_abbr, county_id, county_name)
        
        if not table_rows:
            print(f"No records found for {county_name} County, {state_name}")
            continue
        
        # Count total records (accounting for multi-parcel patents)
        total_records = count_unique_patents(table_rows)
        print(f"Found {len(table_rows)} total rows representing approximately {total_records} unique patents")
        
        # Initialize records array
        enhanced_records = []
        
        # Process records one by one
        current_index = 0
        max_records = 10  # Limit for testing
        processed_records = 0
        
        while processed_records < max_records and current_index < len(table_rows):
            print(f"Processing record {processed_records + 1} at table index {current_index}")
            
            # Process a single record from the table rows
            result = process_record_from_rows(table_rows, current_index, state_abbr, county_id)
            
            if not result or not result[0]:
                print(f"Failed to process record at index {current_index}")
                current_index += 1  # Skip this row and try the next one
                continue
                
            record, rows_consumed = result
            
            # Update index for next iteration
            current_index += rows_consumed
            
            # Add delay to avoid overloading the server
            time.sleep(1)
            
            # Enhance record with details
            enhanced_record = enhance_record_with_details(record, state_name)
            
            # Download patent image
            if enhanced_record["document_access"]["image_link"]:
                print(f"Downloading image for record {processed_records + 1}")
                download_patent_image(enhanced_record, county_images_dir)
            
            # Add to our collection
            enhanced_records.append(enhanced_record)
            processed_records += 1
            
            # Save the current state after each record
            with open(file_path, "w") as f:
                json.dump(enhanced_records, f, indent=2)
            
            # Update and save county mapping after each record
            county_data = {
                "state": state_name,
                "state_abbreviation": state_abbr,
                "county_id": county_id,
                "county_name": county_name,
                "file_path": os.path.join("json", state_abbr, file_name),
                "images_directory": os.path.join("images", state_abbr, county_id),
                "total_records": total_records,
                "processed_records": processed_records,
                "completion_percentage": round((processed_records / total_records) * 100, 2) if total_records > 0 else 0,
                "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            # Update or add county mapping
            county_exists = False
            for i, county_map in enumerate(county_mapping):
                if county_map["county_id"] == county_id and county_map["state_abbreviation"] == state_abbr:
                    county_mapping[i] = county_data
                    county_exists = True
                    break
            
            if not county_exists:
                county_mapping.append(county_data)
                
            # Save county mapping file after each record
            mapping_file = os.path.join(base_dir, "county_mapping.json")
            with open(mapping_file, "w") as f:
                json.dump(county_mapping, f, indent=2)
            
            print(f"Saved {processed_records}/{total_records} records ({county_data['completion_percentage']}%) so far to {file_path}")
        
        print(f"Completed processing {len(enhanced_records)}/{total_records} records for {county_name} County, {state_name}")
        
        # For testing, we're only processing one county
        break
    
    print(f"Saved county mapping to {os.path.join(base_dir, 'county_mapping.json')}")
    
    # Create a zip file of the entire dataset (commented out for testing)
    # create_zip(base_dir, "land_records_complete")

if __name__ == "__main__":
    main()