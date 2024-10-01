from SPARQLWrapper import SPARQLWrapper, JSON, XML, N3, RDF, CSV, TSV
from IPython.display import clear_output
import csv

ENDPOINT = "http://dbpedia.org/sparql"
CSV_PATH = "data.csv"
QUERY_LIMIT = 10000

q = """
PREFIX dbo: <http://dbpedia.org/ontology/>
PREFIX dbp: <http://dbpedia.org/property/>
PREFIX dbr: <http://dbpedia.org/resource/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?player ?name ?birthDate ?positionLabel ?nationality 
       ?height ?weight ?birthPlace ?number ?wikiPage
WHERE {
  ?player a dbo:SoccerPlayer ;
          foaf:name ?name .
  ?player dbo:birthDate ?birthDate .
  
  # Limiting birthdate to less than 40 years ago
  FILTER (?birthDate >= (NOW() - "P40Y"^^xsd:duration))

  OPTIONAL { 
    ?player dbo:position ?position . 
    ?position rdfs:label ?positionLabel . 
    FILTER(LANG(?positionLabel) = "en")
  }

  OPTIONAL { ?player dbo:nationality ?nationality . }
  OPTIONAL { ?player dbo:height ?height .  }
  OPTIONAL { ?player dbo:weight ?weight . }
  OPTIONAL { ?player dbo:birthPlace ?birthPlace . }
  OPTIONAL { ?player dbp:number ?number .  }
  OPTIONAL { ?player foaf:isPrimaryTopicOf ?wikiPage . }
}
"""
q += f"\nLIMIT 10000"

sparql = SPARQLWrapper(ENDPOINT)

# clear the file
f = open(CSV_PATH, 'wb')
f.write(b"")
f.close()

def query(offset=0):
    mod_q = q + f"\nOFFSET {offset}"
    sparql.setQuery(mod_q)
    sparql.setReturnFormat(CSV)
    results = sparql.query().convert()
    return results

def write(bin_data):
    with open(CSV_PATH, "ab") as bf:
        bf.write(bin_data)
        bf.close()
        
def lines(filepath = CSV_PATH):
    return sum(1 for _ in open(filepath))
    
def remove_duplicate_headers(input_file = CSV_PATH, output_file = f"{CSV_PATH}-cleaned.csv"):
    with open(input_file, 'r', newline='', encoding='utf-8') as infile:
        reader = csv.reader(infile)
        header = next(reader)  # Read the header row
        rows = [header]  # Start with the header row
        seen = set()
        seen.add(tuple(header))  # Track the header as a tuple to compare with other rows
        
        for row in reader:
            # Convert the row to a tuple and check if it matches the header
            if tuple(row) != tuple(header):
                rows.append(row)  # Add only non-header rows
                
    # Write the cleaned data to the output file
    with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        writer.writerows(rows)
        
    return output_file

##################
# ACTUAL PROGRAM #
##################

i = 0
while True:
    offset = i * 10000
    
    print(f"\nquerying offset {offset}")
    res = query(offset=offset)
    
    clear_output(wait=True)
    
    if len(res) < 500:
        print(f"got no more results after {i} iterations")
        print(f"total data scraped: {lines()} rows")
        clean_file_name = remove_duplicate_headers(CSV_PATH)
        print(f"total data scraped after clean: {lines(clean_file_name)} rows")
        break
              
    print(f"got {len(res)} bytes")
    print(f"new length: {lines()}")
    
    write(res)
    i += 1
