from bs4 import BeautifulSoup

def parse_patentees(patentee_cell):
    """
    Parse patentee names from a table cell, handling multiple patentees
    separated by line breaks or commas. Also identifies different types
    of people (patentee, warrantee, etc.) based on image indicators.
    
    Args:
        patentee_cell: BeautifulSoup cell element containing patentee names
        
    Returns:
        List of dictionaries, each containing a patentee name, type, and additional_info
    """
    patentees = []
    
    # Get the HTML content to handle <br> tags and images
    patentee_html = str(patentee_cell)
    
    # Replace <br> tags with a special marker
    patentee_html = patentee_html.replace('<br>', '|BREAK|')
    patentee_html = patentee_html.replace('<br/>', '|BREAK|')
    patentee_html = patentee_html.replace('<br />', '|BREAK|')
    
    # Create a new soup object to get the text without HTML tags
    soup = BeautifulSoup(patentee_html, 'html.parser')
    
    # Split the HTML by break markers to process each line
    html_parts = patentee_html.split('|BREAK|')
    text_parts = soup.text.strip().split('|BREAK|')
    
    # Process each part
    for i, part in enumerate(text_parts):
        part = part.strip()
        if not part or part.startswith('Patentee') or part.startswith('Warrantee'):
            continue
            
        # Determine the type based on image in the HTML
        person_type = "unknown"
        html_before = html_parts[i] if i < len(html_parts) else ""
        
        if 'patentee.png' in html_before:
            person_type = "patentee"
        elif 'warrantee.png' in html_before:
            person_type = "warrantee"
        elif 'assignee.png' in html_before:
            person_type = "assignee"
        elif 'widow.png' in html_before:
            person_type = "widow"
        elif 'heir.png' in html_before:
            person_type = "heir"
        
        # Further split by commas if needed (for cases like "SMITH, JOHN")
        # but be careful not to split names that naturally contain commas
        names = [name.strip() for name in part.split(',') if name.strip()]
        
        # If we have multiple comma-separated items, they might be parts of one name
        # or multiple names. Use heuristics to decide.
        if len(names) > 1:
            # Check if this looks like a single name (e.g., "SMITH, JOHN")
            if len(names) == 2 and len(names[1].split()) <= 2:
                # This is likely a single name in "Last, First" format
                patentees.append({
                    "name": part.strip(),
                    "type": person_type,
                    "additional_info": None
                })
            else:
                # These are likely multiple names
                for name in names:
                    if name.strip():
                        patentees.append({
                            "name": name.strip(),
                            "type": person_type,
                            "additional_info": None
                        })
        else:
            # Just one name
            patentees.append({
                "name": part.strip(),
                "type": person_type,
                "additional_info": None
            })
    
    # If no patentees were added (unlikely), add the original text as fallback
    if not patentees:
        patentees.append({
            "name": patentee_cell.text.strip(),
            "type": "unknown",
            "additional_info": None
        })
    
    return patentees
